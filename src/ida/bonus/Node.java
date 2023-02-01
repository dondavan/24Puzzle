package ida.bonus;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.SendPort;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
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
    static int REQUEST_DENY = 7;
    static int SEND_BOARD = 6;
    static int RECV_BOARD = 5;
    static int SERVER_READY = 3;
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

    // Set to 1 when result found
    private int result = 0;
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
    ArrayBlockingQueue<IbisIdentifier> forkSet;
    ArrayBlockingQueue<IbisIdentifier> peerSet;

    // Coordiante state
    private boolean waitingMessage = true;
    private boolean waitingServer = true;
    private boolean useCache;
    private int solveResult;

    // Timer
    long start;
    long end;

    public Node(Ibis ibis, Board initial)throws Exception{

        waitingMessageSet = new ArrayBlockingQueue<IbisIdentifier>(50);
        computingSet = new ArrayBlockingQueue<IbisIdentifier>(50);
        doneSet = new ArrayBlockingQueue<IbisIdentifier>(50);
        forkSet = new ArrayBlockingQueue<IbisIdentifier>(50);
        peerSet = new ArrayBlockingQueue<IbisIdentifier>(50);

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
        end = System.currentTimeMillis();
        ibis.end();
        System.err.println("ida took " + (end - start) + " milliseconds");
    }

    private void run() throws IOException, InterruptedException {


        if(initialBoard == null){
            broadcastMessage(0,0,SEND_BOARD);
            // Wait till job received
            while(jobQueue.isEmpty()){
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

        while (result == 0) {

            if(initialBoard != null){
                initialBoard.setBound(bound);
                jobQueue.add(initialBoard);
                System.out.print(bound + " ");
                System.out.flush();
            }
            expansions = 0;

            // Bound iteration finishes when job queue  is empty and all client done computing
            synchronized (this) {

                while (!jobQueue.isEmpty()) {
                    Board board = jobQueue.poll();
                    result = solve(board,true);
                }

                // Wait till client finishes job
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
        System.out.println("\nresult is " + result + " solutions of "
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
            if(working||!jobQueue.isEmpty()){
                done2Wait(identifier);
                wakeUp();
            }else {
            }

        }
        else if(peerMessage[peerMessage.length-1] == RECV_BOARD){
            Board newboard = new Board(peerMessage);
            jobQueue.add(newboard);
            if(peerSet.isEmpty()){
                peerSet.add(identifier);
            }
            System.err.println(newboard);
        }
        // Result Found
        else if(peerMessage[peerMessage.length-1] == RESULT_FOUND){
            ByteBuffer buf = ByteBuffer.wrap(peerMessage);
            result += buf.getInt(0);
            expansions += buf.getInt(8);
            comp2Done(identifier);
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
        //  Client result not found
        else if(peerMessage[peerMessage.length-1] == RESULT_NOT_FOUND){
            ByteBuffer buf = ByteBuffer.wrap(peerMessage);
            expansions += buf.getInt(8);
            comp2Done(identifier);
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
        //System.err.println("Queue Job Depth: " + board.depth());
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
            if((sendPort.connectedTo())[0].ibisIdentifier().equals(target)){
                WriteMessage w = sendPort.newMessage();
                w.writeArray(byteBoard);
                w.finish();
                forkSet.add(target);
                wait2Comp(target);
                System.err.println("SEND to "+ target );
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
                System.err.println(sendPort.connectedTo());
            }
            waitingMessage = true;
        }
    }

    private synchronized int solve(Board board, boolean useCache) throws IOException {
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
            return 1;
        }

        if (board.distance() > board.bound()) {
            return 0;
        }

        if(jobQueue.size() != 0 && !waitingMessageSet.isEmpty()){
            Iterator ID_iterator = waitingMessageSet.iterator();
            while (ID_iterator.hasNext() && !jobQueue.isEmpty()) {
                IbisIdentifier target = (IbisIdentifier) ID_iterator.next();
                sendBoard(target);
            }
        }
        Board[] children = board.makeMoves(cache, board.depth());
        int result = 0;

        if(board.depth() < MAXHOP){

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
            if(targetBoard!=null) {
                result += solutions(targetBoard,cache);
            }
        }else {
            for (int i = 0; i < children.length; i++) {
                if (children[i] != null) {
                    result += solutions(children[i], cache);
                }
            }
        }

        cache.put(children);
        return result;
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
        int result = 0;
        for (int i = 0; i < children.length; i++) {
            if (children[i] != null) {
                result += solutions(children[i]);
            }
        }
        return result;
    }



    public void setFinished() throws IOException {
        for (SendPort sendPort :sendPorts){
            byte[] bytes = new byte[1];
            Integer intResult = new Integer(END);
            bytes[0] = intResult.byteValue();
            WriteMessage w = sendPort.newMessage();
            w.writeArray(bytes);
            w.finish();
        }

        for (SendPort sendPort :sendPorts){
            // Close ports.
            sendPort.close();
        }

        // Close receive port.
        for (ReceivePort receivePort :receivePorts){
            // Close ports.
            receivePort.close();
        }
    }

    /**
     * Move target from waiting set to computing set
     * @param target
     */
    synchronized void wait2Comp(IbisIdentifier target){
        waitingMessageSet.remove(target);
        computingSet.add(target);
    }

    /**
     * Move target from computing set to done set
     * @param target
     */
    synchronized void comp2Done(IbisIdentifier target){
        computingSet.remove(target);
        doneSet.add(target);
    }

    /**
     * Move target from waiting set to computing set
     * @param target
     */
    synchronized void done2Wait(IbisIdentifier target){
        doneSet.remove(target);
        waitingMessageSet.add(target);
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
