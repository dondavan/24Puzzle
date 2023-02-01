package ida.bonus;

import ibis.ipl.*;
import ida.ipl.Board;
import ida.ipl.BoardCache;
import ida.ipl.Ida;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;

import static ida.ipl.Board.NSQRT;

/**
 * Ibis server, responsible for distributing board branch to client
 */
public class Server implements MessageUpcall{

    /**
     *   Flag for client message
     **/
    static int END = -1;
    static int SEND_BOARD = 6;
    static int RECV_BOARD = 5;
    static int SERVER_READY = 3;
    static int RESULT_BOARD = 2;
    static int RESULT_FOUND = 1;
    static int RESULT_NOT_FOUND = 0;

    static int MAXHOP = 6;

    /**
     *  Ibis properties
     **/
    private Ibis ibis;


    static int QUEUE_SIZE = 10000;
    private ArrayList<SendPort> sendPorts;          // Save send port to client
    private ArrayBlockingQueue<ida.ipl.Board> jobQueue;     // Job queue
    private ReceivePort receivePort;
    private ida.ipl.Board initialBoard;

    // Set to 1 when result found
    private int result = 0;
    private int bound;
    static int expansions;


    // Client Status Set
    int clientSize = 0;
    ArrayBlockingQueue<IbisIdentifier> waitingMessageSet;
    ArrayBlockingQueue<IbisIdentifier> computingSet;
    ArrayBlockingQueue<IbisIdentifier> doneSet;

    // Timer
    long start;
    long end;

    int count = 0;
    public Server(Ibis ibis, ida.ipl.Board initial) throws Exception{

        waitingMessageSet = new ArrayBlockingQueue<IbisIdentifier>(50);
        computingSet = new ArrayBlockingQueue<IbisIdentifier>(50);
        doneSet = new ArrayBlockingQueue<IbisIdentifier>(50);

        /* Assign ibis instance. */
        this.ibis = ibis;
        sendPorts = new ArrayList<SendPort>();
        /* Store initial board in job queue*/
        this.initialBoard = initial;
        jobQueue = new ArrayBlockingQueue<ida.ipl.Board>(QUEUE_SIZE);

        ibis.registry().waitUntilPoolClosed();
        IbisIdentifier[] joinedIbises = ibis.registry().joinedIbises();

        // Connnect to client
        receiverConnect();
        senderConnect(joinedIbises);

        System.out.println("Running IDA*, initial board:");
        System.out.println(initialBoard);

        start = System.currentTimeMillis();
        run();
        setFinished();
        end = System.currentTimeMillis();
        ibis.end();
        System.err.println("ida took " + (end - start) + " milliseconds");
        System.err.println("Server count "+ count);
    }

    private void run() throws IOException {
        bound = initialBoard.distance();
        serverReady();

        System.out.print("Try bound ");
        System.out.flush();

        while (result == 0) {
            initialBoard.setBound(bound);
            jobQueue.add(initialBoard);

            System.out.print(bound + " ");
            System.out.flush();

            expansions = 0;
            count = 0;
            // Bound iteration finishes when job queue  is empty and all client done computing
            synchronized (this) {

                while (!jobQueue.isEmpty() && result == 0) {

                    // Master generate job
                    ida.ipl.Board board = jobQueue.poll();
                    result = solve(board,true);

                    // Wait till client request for job
                    while(waitingMessageSet.isEmpty() && result == 0){
                        try {
                            wait();
                        } catch (Exception e) {
                            // ignored
                        }
                    }

                    // Send job, if job Queue is not empty, and client waiting message
                    while(!jobQueue.isEmpty() && !waitingMessageSet.isEmpty() && result == 0){
                        Iterator ID_iterator = waitingMessageSet.iterator();
                        while (ID_iterator.hasNext() && !jobQueue.isEmpty()) {
                            IbisIdentifier target = (IbisIdentifier) ID_iterator.next();
                            sendBoard(target);
                        }
                    }

                    // Wait till client finishes job
                    while(computingSet.size() != 0 && result == 0){
                        try {
                            wait();
                        } catch (Exception e) {
                            // ignored
                        }
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
        byte[] clientMessage = (byte[]) message.readObject();
        int flag = clientMessage[clientMessage.length-1];
        IbisIdentifier identifier = message.origin().ibisIdentifier();
        //System.err.println("Recv from:" + identifier + "Flag: "+ flag);
        // Client send children board

        if(clientMessage[clientMessage.length-1] == SEND_BOARD){

            //System.err.println("===  Waiting "+ waitingMessageSet + " Computing "+ computingSet+ " Done "+ doneSet );
            done2Wait(identifier);
            wakeUp();
        }
        else if(clientMessage[clientMessage.length-1] == RECV_BOARD){
            byte[] byteBoard = new byte[NSQRT*NSQRT + 4];
            int count = clientMessage[clientMessage.length-2];
            //System.err.println("Count: "+ count);
            for(int i = 0; i < count; i++ ){
                for(int j = 0; j < NSQRT*NSQRT + 4 ; j++){
                    byteBoard[j] = clientMessage[(NSQRT*NSQRT + 4) * i +j];
                }
                ida.ipl.Board board = new ida.ipl.Board(byteBoard);
                jobQueue.add(board);
            }
        }
        // Result Found
        else if(clientMessage[clientMessage.length-1] == RESULT_FOUND){
            ByteBuffer buf = ByteBuffer.wrap(clientMessage);
            expansions += buf.getInt(0);
            comp2Done(identifier);
            result = clientMessage[clientMessage.length-1];
            wakeUp();
        }
        // Receive result board
        else if(clientMessage[clientMessage.length-1] == RESULT_BOARD){
            System.err.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>> Gotcha!");
            byte[] byteBoard = new byte[NSQRT*NSQRT + 4];
            int count = clientMessage[clientMessage.length-2];
            //System.err.println("Count: "+ count);
            for(int i = 0; i < count; i++ ){
                for(int j = 0; j < NSQRT*NSQRT + 4 ; j++){
                    byteBoard[j] = clientMessage[(NSQRT*NSQRT + 4) * i +j];
                }
                ida.ipl.Board board = new ida.ipl.Board(byteBoard);
                System.err.println(board);
            }
        }
        //  Client result not found
        else if(clientMessage[clientMessage.length-1] == RESULT_NOT_FOUND){
            ByteBuffer buf = ByteBuffer.wrap(clientMessage);
            expansions += buf.getInt(0);
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
        ida.ipl.Board board = jobQueue.poll();
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
        Integer intFlag = new Integer(SEND_BOARD);
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
                wait2Comp(target);
            }
        }


    }


    private synchronized int solve(ida.ipl.Board board, boolean useCache) throws IOException {
        ida.ipl.BoardCache cache = null;
        if (useCache) {
            cache = new ida.ipl.BoardCache();
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
    private int solutions(ida.ipl.Board board, BoardCache cache) throws IOException {
        expansions++;
        count ++;
        if (board.distance() == 0) {
            System.err.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>> Gotcha!");
            System.err.println(board);
            return 1;
        }

        if (board.distance() > board.bound()) {
            return 0;
        }

        ida.ipl.Board[] children = board.makeMoves(cache, board.depth());
        int result = 0;

        if(board.depth() < MAXHOP){

            ida.ipl.Board targetBoard = null;
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
            while(!jobQueue.isEmpty() && !waitingMessageSet.isEmpty()){
                Iterator ID_iterator = waitingMessageSet.iterator();
                while (ID_iterator.hasNext() && !jobQueue.isEmpty()) {
                    IbisIdentifier target = (IbisIdentifier) ID_iterator.next();
                    sendBoard(target);
                }
            }
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
    private int solutions(ida.ipl.Board board) {
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

    private void serverReady() throws IOException {
        for (SendPort sendPort :sendPorts){
            byte[] byteBoard = new byte[1];
            Integer intServer = new Integer(SERVER_READY);
            byteBoard[0] = intServer.byteValue();
            WriteMessage w = sendPort.newMessage();
            w.writeArray(byteBoard);
            w.finish();
        }
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

        // Close receive port.
        for (SendPort sendPort :sendPorts){
            // Close ports.
            sendPort.close();
        }

        receivePort.close();
    }

    /**
     * Send port connect to client
     * @param ibisIdentifiers
     * @throws Exception
     */
    private void senderConnect(IbisIdentifier[] ibisIdentifiers) throws Exception {
        for(IbisIdentifier identifier:ibisIdentifiers){
            if(!identifier.equals(ibis.identifier())){
                SendPort sendPort = ibis.createSendPort(ida.ipl.Ida.ONE2MANY);
                ReceivePortIdentifier clientPortId = sendPort.connect(identifier, "fromServer");
                sendPorts.add(sendPort);
                clientSize++;
            }
        }
    }

    /**
     * Receive port connect to client
     * @throws IOException
     */
    private void receiverConnect() throws IOException {
        receivePort = ibis.createReceivePort(ida.ipl.Ida.MANY2ONE, "toServer",this);
        // enable connections
        receivePort.enableConnections();
        // enable upcalls
        receivePort.enableMessageUpcalls();
    }
}
