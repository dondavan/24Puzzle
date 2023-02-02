package ida.bonus;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.SendPort;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;

import static ida.bonus.Board.NSQRT;
import static ida.bonus.Ida.*;

import ibis.ipl.*;

public class Node  implements MessageUpcall{
    /**
     *   Flag for client message
     **/
    static int END = -1;
    static int OPEN_REQUEST = 7;
    static int SEND_BOARD = 6;
    static int RECV_BOARD = 5;
    static int RESULT_BOARD = 2;
    static int RESULT_FOUND = 1;
    static int RESULT_NOT_FOUND = 0;

    static int MAXHOP = 6;


    //Ibis properties
    private Ibis ibis;


    static int QUEUE_SIZE = 10000;
    private ArrayList<SendPort> sendPorts;          // Save send port to client
    private ArrayBlockingQueue<Board> jobQueue;     // Job queue
    private ArrayList<ReceivePort> receivePorts;
    private Board initialBoard;

    // Total result
    private int result = 0;
    // Local result
    private int sovleResult = 0;
    // Set to false when result found
    private boolean running = true;
    // Current node has work to do
    private boolean working = false;
    private int shortest_slides = 0;
    private int bound;
    static int expansions;


    // Client Status Set
    int clientSize = 0;
    ArrayBlockingQueue<IbisIdentifier> waitingMessageSet;
    ArrayBlockingQueue<IbisIdentifier> computingSet;
    ArrayBlockingQueue<IbisIdentifier> doneSet;

    ArrayBlockingQueue<IbisIdentifier> workingSet;
    ArrayBlockingQueue<IbisIdentifier> employerSet;
    ArrayBlockingQueue<IbisIdentifier> forkSet;
    ArrayBlockingQueue<IbisIdentifier> peerSet;
    ArrayBlockingQueue<IbisIdentifier> sendPendingSet;

    // Coordiante state
    private boolean waitingMessage = true;

    // Timer
    long start;
    long end;

    public Node(Ibis ibis, Board initial)throws Exception{

        waitingMessageSet = new ArrayBlockingQueue<IbisIdentifier>(50);
        workingSet = new ArrayBlockingQueue<IbisIdentifier>(50);
        forkSet = new ArrayBlockingQueue<IbisIdentifier>(50);
        peerSet = new ArrayBlockingQueue<IbisIdentifier>(50);
        employerSet = new ArrayBlockingQueue<IbisIdentifier>(50);
        sendPendingSet = new ArrayBlockingQueue<IbisIdentifier>(50);

        /* Assign ibis instance. */
        this.ibis = ibis;
        sendPorts = new ArrayList<SendPort>();
        receivePorts = new ArrayList<ReceivePort>();

        /* Store initial board in job queue*/
        this.initialBoard = initial;
        jobQueue = new ArrayBlockingQueue<Board>(QUEUE_SIZE);

        ibis.registry().waitUntilPoolClosed();
        IbisIdentifier[] joinedIbises = ibis.registry().joinedIbises();

        // Connnect to other peers
        portInit(joinedIbises);

        System.out.println("Running IDA*, initial board:");
        System.out.println(initialBoard);

        start = System.currentTimeMillis();
        run();
        setFinished();
        System.err.println("finished");
        end = System.currentTimeMillis();
        ibis.end();
        System.err.println("ida took " + (end - start) + " milliseconds");
    }

    private void run() throws IOException {

        if(initialBoard == null){
            // Wait till any node is open to request
            while(workingSet.isEmpty()){
                try {
                    wait();
                } catch (Exception e) {
                    // ignored
                }
            }
        }else {
            bound = initialBoard.distance();
            shortest_slides = bound;
            System.out.print("Try bound ");
            System.out.flush();

        }


        /*

        Do not send job to employeer when still computing job from employeer



         */
        while (result == 0 && running) {

            if(initialBoard != null){
                initialBoard.setBound(bound);
                jobQueue.add(initialBoard);
                System.out.print(bound + " ");
                System.out.flush();
            }else {
                // Steal work from peer
                IbisIdentifier target =  workingSet.poll();
                sendMessage(0,0,SEND_BOARD,target);
                while(waitingMessage){
                    try {
                        wait();
                    } catch (Exception e) {
                        // ignored
                    }
                }
            }
            broadcastMessage(0,0,OPEN_REQUEST);

            expansions = 0;

            // Bound iteration finishes when job queue  is empty and all client done computing
            synchronized (this) {

                while (!jobQueue.isEmpty()) {
                    Board board = jobQueue.poll();
                    sovleResult = solve(board,true);
                    result += sovleResult;
                }
                // Send job back to original fork
                if(employerSet.size() != 0){
                    Iterator<IbisIdentifier> ID_iterator = employerSet.iterator();
                    while (ID_iterator.hasNext()) {
                        IbisIdentifier target = ID_iterator.next();
                        if(result ==0){
                            sendMessage(result,expansions,RESULT_NOT_FOUND,target);
                        }else {
                            sendMessage(result,expansions,RESULT_FOUND,target);
                        }
                        employerSet.remove(target);
                    }
                }
                // Wait till all fork finishes job
                while(forkSet.size() != 0){
                    try {
                        wait();
                    } catch (Exception e) {
                        // ignored
                    }
                }
            }

            bound += 2;
            System.err.println("Expansions: " + expansions);
        }
        if(initialBoard != null)System.out.println("\nresult is " + result + " solutions of "
                + initialBoard.bound() + " steps");

    }

    /**
     * Override upcall to implement messageupcall
     * Automatic triggered when new message incoming
     * One byte for signal transmission
     * otherwise for board transmisson
     * @param message
     * @throws IOException
     */
    @Override
    public void upcall(ReadMessage message) throws IOException, ClassNotFoundException {
        byte[] peerMessage = (byte[]) message.readObject();
        IbisIdentifier identifier = message.origin().ibisIdentifier();
        // Client request for job
        if(peerMessage[peerMessage.length-1] == SEND_BOARD){
            if(working && !waitingMessageSet.contains(identifier)){
                waitingMessageSet.add(identifier);
            }
        }
        else if(peerMessage[peerMessage.length-1] == OPEN_REQUEST){
            if(!workingSet.contains(identifier)){
                workingSet.add(identifier);
            }
            System.err.println(identifier + " is open");
            wakeUp();
        }
        else if(peerMessage[peerMessage.length-1] == RECV_BOARD){
            waitingMessage = false;
            Board newboard = new Board(peerMessage);
            jobQueue.add(newboard);
            employerSet.add(identifier);
        }
        // Result Found
        else if(peerMessage[peerMessage.length-1] == RESULT_FOUND){
            ByteBuffer buf = ByteBuffer.wrap(peerMessage);
            result += buf.getInt(0);
            expansions += buf.getInt(8);
            forkSet.remove(identifier);
            end = System.currentTimeMillis();
            wakeUp();
        }
        // Receive result board
        else if(peerMessage[peerMessage.length-1] == RESULT_BOARD){
            System.err.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>> Gotcha!");
            byte[] byteBoard = new byte[NSQRT* NSQRT + 4];
            int count = peerMessage[peerMessage.length-2];
            for(int i = 0; i < count; i++ ){
                for(int j = 0; j < NSQRT* NSQRT + 4 ; j++){
                    byteBoard[j] = peerMessage[(NSQRT* NSQRT + 4) * i +j];
                }
                Board board = new Board(byteBoard);
                System.err.println(board);
                if(board.depth() < shortest_slides){
                    shortest_slides = board.depth();
                }
            }
        }
        //  Peer work result not found
        else if(peerMessage[peerMessage.length-1] == RESULT_NOT_FOUND){
            ByteBuffer buf = ByteBuffer.wrap(peerMessage);
            expansions += buf.getInt(8);
            forkSet.remove(identifier);
            wakeUp();
        }// END signal
        else if(peerMessage[peerMessage.length-1] == END){
            running = false;
            wakeUp();
        }

        message.finish();
    }



    /**
     * Send pending board on jobqueue to target client
     * @param target
     * @throws IOException
     */
    private synchronized void sendBoard(IbisIdentifier target) throws IOException {
        Board board = jobQueue.poll();
        byte[] byteBoard = new byte[NSQRT*NSQRT + 5];
        byte[] byteBuffer = board.getByteBoard();
        for(int i =0;i<byteBuffer.length;i++){
            byteBoard[i] = byteBuffer[i];
        }
        Integer intPrevX = new Integer(board.getPrevX());
        Integer intPrevY = new Integer(board.getPrevY());
        Integer intBound = new Integer(board.bound());
        Integer intDepth = new Integer(board.depth());
        Integer intFlag = new Integer(RECV_BOARD);
        byteBoard[NSQRT * NSQRT] = intPrevX.byteValue();
        byteBoard[NSQRT * NSQRT + 1] = intPrevY.byteValue();
        byteBoard[NSQRT * NSQRT + 2] = intBound.byteValue();
        byteBoard[NSQRT * NSQRT + 3] = intDepth.byteValue();
        byteBoard[NSQRT * NSQRT + 4] = intFlag.byteValue();

        for (SendPort sendPort :sendPorts){
            for(int i = 0; i < sendPort.connectedTo().length; i++){
                if((sendPort.connectedTo())[i].ibisIdentifier().equals(target)){
                    WriteMessage w = sendPort.newMessage();
                    w.writeArray(byteBoard);
                    w.finish();
                    forkSet.add(target);
                    System.err.println("SEND to "+ target );
                }
            }
        }
    }

    /**
     * Broadcast message to all peer nodes
     * @param result
     * @param expansion
     * @param flag
     * @throws IOException
     */
    private synchronized void broadcastMessage(int result, int expansion,int flag) throws IOException {
        // result + expansion + flag
        byte[] bytes = new byte[24];

        ByteBuffer buf = ByteBuffer.wrap(bytes);
        Integer flagInt = flag;
        byte flagByte = flagInt.byteValue();
        buf.putInt(result);
        buf.putInt(8,expansion);
        buf.put(bytes.length-1,flagByte);

        if (running) {
            for (SendPort sendPort :sendPorts){
                WriteMessage w = sendPort.newMessage();
                w.writeArray(bytes);
                w.finish();
            }
            waitingMessage = true;
        }
    }

    private synchronized void sendMessage(int result, int expansion,int flag,IbisIdentifier target) throws IOException {
        // result + expansion + flag
        byte[] bytes = new byte[24];

        ByteBuffer buf = ByteBuffer.wrap(bytes);
        Integer flagInt = flag;
        byte flagByte = flagInt.byteValue();
        buf.putInt(result);
        buf.putInt(8,expansion);
        buf.put(bytes.length-1,flagByte);

        if (running) {
            for (SendPort sendPort :sendPorts){
                if((sendPort.connectedTo())[0].ibisIdentifier().equals(target)){
                    WriteMessage w = sendPort.newMessage();
                    w.writeArray(bytes);
                    w.finish();
                    waitingMessage = true;
                    System.err.println("Send to " + target + "  " +result + " FLag" + flag);
                }
            }
        }
    }

    private int solve(Board board, boolean useCache) throws IOException {
        BoardCache cache = null;
        working = true;
        if (useCache) {
            cache = new BoardCache();
        }
        int solutions;

        if (useCache) {
            solutions = solutions(board, cache);
        } else {
            solutions = solutions(board);
        }
        return solutions;

    }

    /**
     * expands this board into all possible positions, and returns the number of
     * solutions. Will cut off at the bound set in the board.
     */
    private int solutions(Board board, BoardCache cache) throws IOException {
        expansions++;
        if (board.distance() == 0) {
            System.err.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>> Gotcha!");
            System.err.println(board);
            /*
            Board[] result = new Board[1];
            result[0] = board;
            sendResultBoard(result,RESULT_BOARD);
             */
            return 1;
        }

        if (board.distance() > board.bound()) {
            return 0;
        }

        Board[] children = board.makeMoves(cache, board.depth());
        int solutions = 0 ;

        if(!waitingMessageSet.isEmpty() && jobQueue.size() <= 4){

            Board targetBoard = null;
            int best_dist = board.bound();

            for (int i = 0; i < children.length; i++) {
                if (children[i] != null && children[i].distance() < best_dist) {
                    best_dist = children[i].distance();
                }
                // Cut of unqualified child
                if(children[i] != null && children[i].distance() > children[i].bound()){
                    children[i] = null;
                    expansions++;
                }
            }
            for (int i = 0; i < children.length; i++) {
                if (children[i] != null && children[i].distance() == best_dist && targetBoard == null) {
                    targetBoard = children[i];
                    children[i] = null;
                    break;
                }
            }
            for (int i = 0; i < children.length; i++) {
                if (children[i] != null) {
                    jobQueue.add(children[i]);
                }
            }
            while(!waitingMessageSet.isEmpty() && !jobQueue.isEmpty()) {
                IbisIdentifier target = waitingMessageSet.poll();
                sendBoard(target);
            }


            if(targetBoard!=null) {
                solutions += solutions(targetBoard,cache);
            }

        }else {
            for (int i = 0; i < children.length; i++) {
                if (children[i] != null) {
                    solutions += solutions(children[i], cache);
                }
            }
        }


        cache.put(children);
        return solutions;
    }

    /**
     * expands this board into all possible positions, and returns the number of
     * solutions. Will cut off at the bound set in the board.
     */
    private int solutions(Board board) {
        expansions++;
        if (board.distance() == 0) {
            System.err.println(board);
            return 1;
        }

        if (board.distance() > board.bound()) {
            return 0;
        }

        Board[] children = board.makeMoves(board.depth());
        int solutions = 0;
        for (int i = 0; i < children.length; i++) {
            if (children[i] != null) {
                solutions += solutions(children[i]);
            }
        }
        return solutions;
    }



    public void setFinished() throws IOException {
        broadcastMessage(0,0,END);
        running = false;

        System.err.println("finish called");

    }

    synchronized void wakeUp(){
        notifyAll();
    }

    /**
     * Send port an receive port set up
     * Each instance take turns to set up send port
     * @param ibisIdentifiers
     * @throws IOException
     */
    private void portInit(IbisIdentifier[] ibisIdentifiers) throws IOException {
        for(IbisIdentifier identifierS:ibisIdentifiers){
            if(identifierS.equals(ibis.identifier())){
                SendPort sendPort = ibis.createSendPort(ONE2MANY);

                for(IbisIdentifier identifierR:ibisIdentifiers){
                    if( !identifierR.equals(ibis.identifier()) ){
                        sendPort.connect(identifierR, identifierS.name());
                    }
                }

                while(sendPort.connectedTo().length!=ibisIdentifiers.length-1){
                    //wait
                }
                sendPorts.add(sendPort);
            }
            else {
                ReceivePort receivePort = ibis.createReceivePort(ONE2MANY, identifierS.name(),this);
                // enable connections
                receivePort.enableConnections();
                // enable upcalls
                receivePort.enableMessageUpcalls();
                while(receivePort.connectedTo().length!=1){
                    //wait
                }
                receivePorts.add(receivePort);

            }
        }
        clientSize = ibisIdentifiers.length-1;
    }
    /**
     * Send port connect to client
     * @param ibisIdentifiers
     * @throws Exception
     */


}
