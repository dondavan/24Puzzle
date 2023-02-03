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
    static int CLOSE_REQUEST = 6;
    static int DENY_REQUEST = 5;

    static int RECV_BOARD = 3;
    static int SEND_BOARD = 2;
    static int RESULT_FOUND = 1;
    static int RESULT_NOT_FOUND = 0;

    static int MAXHOP = 20;
    static int PREHOP = 6;
    static int CHUCKSIZE = 5;


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
    // If current node is employee;
    private boolean employee = false;
    private int shortest_slides = 0;
    private int bound;
    private int level;
    static int expansions;


    // Client Status Set
    int clientSize = 0;
    ArrayBlockingQueue<IbisIdentifier> readySet;
    ArrayBlockingQueue<IbisIdentifier> stealSet;
    ArrayBlockingQueue<IbisIdentifier> waitingMessageSet;
    ArrayBlockingQueue<IbisIdentifier> denyMessageSet;
    ArrayBlockingQueue<Integer> doneSet;
    ArrayBlockingQueue<IbisIdentifier> employerSet;
    ArrayBlockingQueue<IbisIdentifier> forkSet;
    ArrayBlockingQueue<IbisIdentifier> peerSet;

    // Coordiante state
    private boolean waitingJob = true;

    // Timer
    long start;
    long end;

    public Node(Ibis ibis, Board initial)throws Exception{

        readySet = new ArrayBlockingQueue<IbisIdentifier>(50);
        stealSet = new ArrayBlockingQueue<IbisIdentifier>(50);
        denyMessageSet = new ArrayBlockingQueue<IbisIdentifier>(50);
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
        ibis.registry().waitUntilPoolClosed();
        IbisIdentifier[] joinedIbises = ibis.registry().joinedIbises();

        // Connnect to other peers
        portInit(joinedIbises);

        if(initialBoard!=null){
            System.out.println("Running IDA*, initial board:");
            System.out.println(initialBoard);
        }

        broadcastMessage(0,0,PEER_READY);
        // Wait till all peers are ready
        while(readySet.size() < joinedIbises.length -1){
            try {
                wait();
            } catch (Exception e) {
                // ignored
            }
        }
        start = System.currentTimeMillis();
        run();
        end = System.currentTimeMillis();
        if(initialBoard != null) System.err.println("ida took " + (end - start) + " milliseconds");
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
            int local_result = 0;

            if(initialBoard != null){
                initialBoard.setBound(bound);
                jobQueue.add(initialBoard);
                System.out.print(bound + " ");
                System.out.flush();
            }else {
                // Steal job from other
                waitingJob = true;
                while(waitingJob && running){
                    try {
                        while (!stealSet.isEmpty()){
                            System.err.println("Requested " + stealSet.peek());
                            sendMessage(0,0,SEND_BOARD,stealSet.poll());
                        }
                    } catch (Exception e) {
                        // ignored
                    }
                }
            }

            expansions = 0;

            broadcastMessage(level,0,OPEN_REQUEST);
            // Bound iteration finishes when job queue  is empty and all client done computing
            while (!jobQueue.isEmpty()) {
                Board board = jobQueue.poll();
                sovleResult = solve(board,true);
                local_result += sovleResult;
            }
            broadcastMessage(level,0,CLOSE_REQUEST);


            result += local_result;

            // Send job back to original fork
            while(employerSet.size() != 0){
                employee = true;
                IbisIdentifier target = employerSet.poll();
                if(local_result ==0){
                    sendMessage(local_result,expansions,RESULT_NOT_FOUND,target);
                }else {
                    sendMessage(0,expansions,RESULT_FOUND,target);
                }
            }

            // If im the root and found result, tell everyone to stop
            if(!employee && result != 0){
                running =false;
                broadcastMessage(0,0,END);
            }

            bound += 2;
            System.err.println("Expansions: " + expansions);
        }
        end = System.currentTimeMillis();
        System.err.println("Found at " + (end - start) + " milliseconds");
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
        //System.err.println("Result "+ result);
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
            stealSet.remove(identifier);
            wakeUp();
        }
        // IF any peer is open to be stealed from
        else if(peerMessage[peerMessage.length-1] == OPEN_REQUEST){
            ByteBuffer buf = ByteBuffer.wrap(peerMessage);
            int peer_level = buf.getInt(0);
            if( (peer_level > level || peer_level == level) )stealSet.add(identifier);
            wakeUp();
        }
        // IF requested client is qualified, send job to target client
        else if(peerMessage[peerMessage.length-1] == SEND_BOARD){
            ByteBuffer buf = ByteBuffer.wrap(peerMessage);
            int peer_level = buf.getInt(0);
            if( (peer_level > level || peer_level == level))waitingMessageSet.add(identifier);
            else denyMessageSet.add(identifier);
            wakeUp();
        }// IF receieved DENY, remove request target from steal list
        else if(peerMessage[peerMessage.length-1] == DENY_REQUEST){
            stealSet.remove(identifier);
            wakeUp();
        }
        else if(peerMessage[peerMessage.length-1] == RECV_BOARD){
            Board newboard = new Board(peerMessage);
            if(!employerSet.contains(identifier))employerSet.add(identifier);
            level = peerMessage[NSQRT * NSQRT + 4] + 1;
            jobQueue.add(newboard);
            waitingJob = false;
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
            forkSet.remove(identifier);
            end = System.currentTimeMillis();
            wakeUp();
        }
        //  Peer work result not found
        else if(peerMessage[peerMessage.length-1] == RESULT_NOT_FOUND){
            ByteBuffer buf = ByteBuffer.wrap(peerMessage);
            forkSet.remove(identifier);
            wakeUp();
        }// END signal
        else if(peerMessage[peerMessage.length-1] == END){
            running = false;
            wakeUp();
        }
        else if(peerMessage[peerMessage.length-1] == PEER_READY){
            readySet.add(identifier);
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

        for (SendPort sendPort :sendPorts){
            if((sendPort.connectedTo())[0].ibisIdentifier().equals(target)){
                WriteMessage w = sendPort.newMessage();
                w.writeArray(bytes);
                w.finish();
            }
        }
    }

    private int solve(Board board, boolean useCache) throws IOException {
        BoardCache cache = null;
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


        while (!denyMessageSet.isEmpty()){
            IbisIdentifier target = denyMessageSet.poll();
            sendMessage(0,0,DENY_REQUEST,target);
        }

        if((!waitingMessageSet.isEmpty() && jobQueue.size() <= waitingMessageSet.size()) || board.depth() < PREHOP){

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

            /*
            while (!waitingMessageSet.isEmpty() && !jobQueue.isEmpty())
                sendBoard(jobQueue.poll(), waitingMessageSet.poll(), RECV_BOARD);
*/
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
