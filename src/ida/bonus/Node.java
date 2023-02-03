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
    static int PEER_READY= 9;
    static int WORK_DONE = 8;

    static int OPEN_REQUEST = 7;
    static int CLOSE_REQUEST = 4;

    static int RETURN_BOARD = 6;
    static int RECV_BOARD = 5;
    static int RESULT_BOARD = 2;
    static int RESULT_FOUND = 1;
    static int RESULT_NOT_FOUND = 0;

    static int MAXHOP = 8;
    static int PREHOP = 8;
    static int CHUCKSIZE = 20;


    //Ibis properties
    private Ibis ibis;


    static int QUEUE_SIZE = 10000;
    private ArrayList<SendPort> sendPorts;          // Save send port to client
    private ArrayBlockingQueue<Board> jobQueue;     // Job queue
    private ArrayBlockingQueue<Board> pushQueue;    // Push queue
    private ArrayBlockingQueue<IbisIdentifier> pushIDQueue;      // Push queue
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
    // If current node is employee;
    private boolean employee = false;
    private int shortest_slides = 0;
    int highest_level;
    int lowest_level;
    IbisIdentifier highest_parent;
    private int bound;
    private int level;
    static int expansions;


    // Client Status Set
    int clientSize = 0;
    int readyPeer = 0;
    ArrayBlockingQueue<IbisIdentifier> waitingMessageSet;
    ArrayBlockingQueue<Integer> doneSet;
    ArrayBlockingQueue<IbisIdentifier> employerSet;
    ArrayBlockingQueue<IbisIdentifier> forkSet;
    ArrayBlockingQueue<IbisIdentifier> peerSet;

    // Coordiante state
    private boolean waitingMessage = true;
    private boolean waitingJob = true;

    // Timer
    long start;
    long end;

    public Node(Ibis ibis, Board initial)throws Exception{

        waitingMessageSet = new ArrayBlockingQueue<IbisIdentifier>(50);
        doneSet = new ArrayBlockingQueue<Integer>(50);
        forkSet = new ArrayBlockingQueue<IbisIdentifier>(50);
        peerSet = new ArrayBlockingQueue<IbisIdentifier>(50);
        employerSet = new ArrayBlockingQueue<IbisIdentifier>(50);

        /* Assign ibis instance. */
        this.ibis = ibis;
        sendPorts = new ArrayList<SendPort>();
        receivePorts = new ArrayList<ReceivePort>();

        /* Store initial board in job queue*/
        this.initialBoard = initial;
        jobQueue = new ArrayBlockingQueue<Board>(QUEUE_SIZE);
        pushQueue = new ArrayBlockingQueue<Board>(1000);
        pushIDQueue = new ArrayBlockingQueue<IbisIdentifier>(1000);

        ibis.registry().waitUntilPoolClosed();
        IbisIdentifier[] joinedIbises = ibis.registry().joinedIbises();

        // Connnect to other peers
        portInit(joinedIbises);

        System.err.println("This " + ibis.identifier());
        System.out.println("Running IDA*, initial board:");
        System.out.println(initialBoard);

        /*
        broadcastMessage(0,0,PEER_READY);
        while(readyPeer < joinedIbises.length -1){
            try {
                wait();
            } catch (Exception e) {
                // ignored
            }
        }*/
        start = System.currentTimeMillis();
        run();
        end = System.currentTimeMillis();
        System.err.println("ida took " + (end - start) + " milliseconds");
        ibis.end();
    }

    private void run() throws IOException {
        if(initialBoard != null){
            bound = initialBoard.distance();
            shortest_slides = bound;
            System.out.print("Try bound ");
            System.out.flush();
        }

        while (running) {
            // Node only give work to level <= its level
            level = 0;
            highest_level = 0;
            lowest_level = 0;
            int local_result = 0;

            if(initialBoard != null){
                initialBoard.setBound(bound);
                jobQueue.add(initialBoard);
                System.out.print(bound + " ");
                System.out.flush();
            }else {
                waitingJob = true;
                broadcastMessage(level,0,OPEN_REQUEST);
                while(waitingJob && running){
                    try {
                        broadcastMessage(level,0,OPEN_REQUEST);
                        Thread.sleep(10);
                    } catch (Exception e) {
                        // ignored
                    }
                }
                broadcastMessage(level,0,CLOSE_REQUEST);
                System.err.println("Got it**********************");
                // Return Extra Board
                while(!pushQueue.isEmpty()){
                    Board board = pushQueue.poll();
                    IbisIdentifier id = pushIDQueue.poll();
                    sendBoard(board,id,RETURN_BOARD);
                }
            }

            expansions = 0;

            // Bound iteration finishes when job queue  is empty and all client done computing
            while (!jobQueue.isEmpty() && running) {
                Board board = jobQueue.poll();
                sovleResult = solve(board,true);
                local_result += sovleResult;
            }


            // Send job back to original fork
            while(employerSet.size() != 0){
                employee = true;
                IbisIdentifier target = employerSet.poll();
                if(local_result ==0){
                    sendMessage(local_result,expansions,RESULT_NOT_FOUND,target);
                }else {
                    // if result found, only send result value to highest parent
                    if(target == highest_parent) sendMessage(result,expansions,RESULT_FOUND,target);
                    else sendMessage(0,expansions,RESULT_FOUND,target);
                }
            }

            result += local_result;

            if(!employee && result != 0){
                running =false;
                // Wait till all fork finishes job
                while(forkSet.size() != 0){
                    try {
                        wait();
                    } catch (Exception e) {
                        // ignored
                    }
                }
                broadcastMessage(0,0,END);
            }

            bound += 2;
            System.err.println("Expansions: " + expansions);
        }
        broadcastMessage(result,0,WORK_DONE);

        // Gather job from peer
        while(doneSet.size() != clientSize){
            try {
                wait();
            } catch (Exception e) {
                // ignored
            }
        }
        while (!doneSet.isEmpty())result+=doneSet.poll();
        System.err.println("Result "+ result);
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
        // Won't save work request from employer
        if(peerMessage[peerMessage.length-1] == CLOSE_REQUEST){
            waitingMessageSet.remove(identifier);
            wakeUp();
        }
        // Prevent deadlock
        else if(peerMessage[peerMessage.length-1] == OPEN_REQUEST){
            ByteBuffer buf = ByteBuffer.wrap(peerMessage);
            int peer_level = buf.getInt(0);
            if( (peer_level > level || peer_level == level)
                    && !jobQueue.isEmpty() && !waitingMessageSet.contains(identifier)) waitingMessageSet.add(identifier);
            wakeUp();
        }
        else if(peerMessage[peerMessage.length-1] == RECV_BOARD){
            Board newboard = new Board(peerMessage);
            if(employerSet.isEmpty()) {
                employerSet.add(identifier);
                level = peerMessage[NSQRT * NSQRT + 4] + 1;
            }
            if(employerSet.isEmpty() || employerSet.contains(identifier)){
                jobQueue.add(newboard);
            }else {
                pushQueue.add(newboard);
                pushIDQueue.add(identifier);
                System.err.println(pushQueue.peek() + "  "+ pushIDQueue.peek() + " Ori "+ identifier);
            }

            waitingJob = false;
        }
        // Push return board back to queue
        else if(peerMessage[peerMessage.length-1] == RETURN_BOARD){
            System.err.println("Pushed " + identifier);
            Board newboard = new Board(peerMessage);
            jobQueue.add(newboard);
        }
        // Won't save work request from employer
        else if(peerMessage[peerMessage.length-1] == WORK_DONE){
            ByteBuffer buf = ByteBuffer.wrap(peerMessage);
            doneSet.add(buf.getInt(0));
            wakeUp();
        }
        // Result Found
        else if(peerMessage[peerMessage.length-1] == RESULT_FOUND){
            ByteBuffer buf = ByteBuffer.wrap(peerMessage);
            result += buf.getInt(0);
            expansions += buf.getInt(8);
            System.err.println("Recv "+ buf.getInt(0) + " from "+ identifier+"=========================");
            forkSet.remove(identifier);
            end = System.currentTimeMillis();
            wakeUp();
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
            waitingMessage = false;
            wakeUp();
        }
        else if(peerMessage[peerMessage.length-1] == PEER_READY){
            readyPeer++;
            wakeUp();
        }

        message.finish();
    }



    /**
     * Send pending board on jobqueue to target client
     * @param target
     * @throws IOException
     */
    private void sendBoard(Board board, IbisIdentifier target,int flag) throws IOException {
        byte[] byteBoard = new byte[NSQRT*NSQRT + 6];
        byte[] byteBuffer = board.getByteBoard();
        for(int i =0;i<byteBuffer.length;i++){
            byteBoard[i] = byteBuffer[i];
        }
        int intPrevX = board.getPrevX();
        int intPrevY = board.getPrevY();
        int intBound = board.bound();
        int intDepth = board.depth();
        byteBoard[NSQRT * NSQRT] = (byte) intPrevX;
        byteBoard[NSQRT * NSQRT + 1] = (byte) intPrevY;
        byteBoard[NSQRT * NSQRT + 2] = (byte) intBound;
        byteBoard[NSQRT * NSQRT + 3] = (byte) intDepth;
        byteBoard[NSQRT * NSQRT + 4] = (byte) level;
        byteBoard[NSQRT * NSQRT + 5] = (byte) flag;

        for (SendPort sendPort :sendPorts){
            if((sendPort.connectedTo())[0].ibisIdentifier().equals(target)){
                WriteMessage w = sendPort.newMessage();
                w.writeArray(byteBoard);
                w.finish();
                if(!forkSet.contains(target))forkSet.add(target);
                //System.err.println("Board Send to "+target);
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
    private void broadcastMessage(int result, int expansion,int flag) throws IOException {
        // result + expansion + flag
        byte[] bytes = new byte[24];
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        Integer flagInt = flag;
        byte flagByte = flagInt.byteValue();
        buf.putInt(result);
        buf.putInt(8,expansion);
        buf.put(bytes.length-1,flagByte);

        try {
            for (SendPort sendPort :sendPorts){
                WriteMessage w = sendPort.newMessage();
                w.writeArray(bytes);
                w.finish();
            }
            waitingMessage = true;
        }catch (IOException e){
            // nothing
        }

    }

    private void sendMessage(int result, int expansion,int flag,IbisIdentifier target) throws IOException {
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
                    System.err.println("Message sent to "+ target +" Result " +result +" Flag "+flag);
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
        if(!running)return 0;
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

        if((!waitingMessageSet.isEmpty() && jobQueue.size() <= waitingMessageSet.size() && board.depth() < MAXHOP) || board.depth() < PREHOP){

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

            while(!waitingMessageSet.isEmpty()) {
                IbisIdentifier target = waitingMessageSet.poll();
                int i =0;
                while(i<jobQueue.size() || i <  CHUCKSIZE){
                    Board board1 = jobQueue.poll();
                    if (board1!=null)sendBoard(board1,target,RECV_BOARD);
                    else break;
                    i++;
                }
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
                for(IbisIdentifier identifierR:ibisIdentifiers){
                    if( !identifierR.equals(ibis.identifier()) ){
                        SendPort sendPort = ibis.createSendPort(ONE2ONE);
                        sendPort.connect(identifierR, identifierS.name()+identifierR.name());
                        sendPorts.add(sendPort);
                    }
                }

            }
            else {
                ReceivePort receivePort = ibis.createReceivePort(ONE2ONE, identifierS.name()+ibis.identifier().name(),this);
                // enable connections
                receivePort.enableConnections();
                // enable upcalls
                receivePort.enableMessageUpcalls();
                while(receivePort.connectedTo().length!=1){
                    // wait
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
