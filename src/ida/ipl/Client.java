package ida.ipl;

import ibis.ipl.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;

import static ida.ipl.Board.NSQRT;
import static ida.ipl.Server.*;


/**
 * Ibis client, receive work from server and explore board
 */
public class Client implements MessageUpcall {

    /**
     *  Ibis properties
     **/
    private ibis.ipl.Ibis ibis;
    private ReceivePort receiver;
    static int expansions;
    IbisIdentifier serverId;
    SendPort sendPort;

    // Set to 1 when result found
    private int result = 0;
    private boolean running = true;


    // Coordiante client status
    private boolean waitingMessage = true;
    private boolean waitingServer = true;
    private boolean useCache;
    private int solveResult;

    private int requestNum = 0;

    private ArrayBlockingQueue<Board> jobQueue;     // Job queue

    long start;

    public Client(ibis.ipl.Ibis ibis,IbisIdentifier serverId,boolean useCache) throws Exception {

        // Assign an ibis instance.
        this.ibis = ibis;
        this.serverId = serverId;
        start = System.currentTimeMillis();
        ibis.registry().waitUntilPoolClosed();
        System.err.println("CLient "+ibis.identifier());

        jobQueue = new ArrayBlockingQueue<Board>(QUEUE_SIZE);

        // Connect to server
        senderConnect();
        receiverConnect();

        this.useCache = useCache;

        run();

        ibis.end();
        System.err.println("Client End");

    }

    private void run() throws IOException {
        waitingServer();
        while (running){
            sendMessage(SEND_BOARD);
            waitingMessage();
            Board board = jobQueue.poll();
            solveResult = solve(board,useCache);
            sendMessage(solveResult);

        }
    }


    public void upcall(ReadMessage message) throws IOException, ClassNotFoundException {
        byte[] bytes = (byte[]) message.readObject();

        if(bytes[bytes.length-1] == END){
            setFinished();
        }else if(bytes[bytes.length-1] == SERVER_READY){
            serverReady();
        }else if(bytes[bytes.length-1] == SEND_BOARD){
            Board newboard = new Board(bytes);
            jobQueue.add(newboard);
            System.err.println("Bound: "+ bytes[NSQRT * NSQRT + 2]);
            System.err.println(newboard);
            messageReady();
        }

        message.finish();
    }


    /**
     * send pending children board to server
     */
    private synchronized void sendBoard(Board[] boards) throws IOException {

        // Size for default 4 board, -1 space for flag, -2 space for board amount
        // Each Section : 0-24 board 25 prevX 26 prevY 27 Bound 28 Depth
        //System.err.println("Sent ");
        byte[] bytes = new byte[ (NSQRT*NSQRT + 4)* 4 + 2];
        int count = 0;
        for (int i = 0; i < boards.length; i++) {
            if (boards[i] != null) {
                byte[] byteBuffer = boards[i].getByteBoard();
                for(int j =0;j<byteBuffer.length;j++){
                    bytes[(NSQRT*NSQRT + 4)* count +j] = byteBuffer[j];
                }
                Integer intPrevX = new Integer(boards[i].getPrevX());
                bytes[(NSQRT*NSQRT + 4)* count + (NSQRT*NSQRT)] = intPrevX.byteValue();
                Integer intPrevY = new Integer(count);
                bytes[(NSQRT*NSQRT + 4)* count + (NSQRT*NSQRT) +1 ] = intPrevY.byteValue();
                Integer intBound = new Integer(boards[i].bound());
                bytes[(NSQRT*NSQRT + 4)* count + (NSQRT*NSQRT) +2 ] = intBound.byteValue();
                Integer intDepth = new Integer(boards[i].depth());
                bytes[(NSQRT*NSQRT + 4)* count + (NSQRT*NSQRT) +3 ] = intDepth.byteValue();

                //System.err.println("Send Board Depth: "+ bytes[(NSQRT*NSQRT + 4)* count + (NSQRT*NSQRT) +3 ] );
                count ++;
            }
        }
        if(count != 0){
            Integer countByte = new Integer(count);
            bytes[ (NSQRT * NSQRT + 4) * 4] = countByte.byteValue();
            Integer flagByte = new Integer(5);
            bytes[ (NSQRT * NSQRT + 4) * 4 + 1] = flagByte.byteValue();

            WriteMessage w = sendPort.newMessage();
            w.writeArray(bytes);
            System.err.println("sent "+ count + "board");
            w.finish();
        }
        waitingMessage = true;
    }

    /**
     * send message to notify server this client is ready
     */
    private synchronized void sendMessage(int flag) throws IOException {
        byte[] bytes = new byte[2];
        Integer flagInt = new Integer(flag);
        Integer numInt = new Integer(requestNum);
        byte numByte = numInt.byteValue();
        byte flagByte = flagInt.byteValue();
        bytes[0] = numByte;
        bytes[1] = flagByte;
        WriteMessage w = sendPort.newMessage();
        w.writeArray(bytes);
        w.finish();
        //System.err.println("Send: "+flag);
        waitingMessage = true;
    }


    private synchronized int solve(Board board, boolean useCache) throws IOException {
        BoardCache cache = null;
        if (useCache) {
            cache = new BoardCache();
        }
        int solutions;

        expansions = 0;
        if (useCache) {
            solutions = solutions(board, cache);
        } else {
            solutions = solutions(board);
        }
        //System.err.println("Result is " + solutions +" Expansions: " + expansions);
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
            System.err.println(board.depth());
            return 1;
        }

        if (board.distance() > board.bound()) {
            return 0;
        }

        Board[] children = board.makeMoves(cache, board.depth());
        int result = 0;

        if(board.depth() < CUT_OFF_DEPTH){

            Board targetBoard = null;
            int best_dist = board.bound();

            for (int i = 0; i < children.length; i++) {
                if (children[i] != null && children[i].distance() < best_dist) {
                    best_dist = children[i].distance();
                }
                // Cut of unqualified child
                /*
                if(children[i] != null && children[i].distance() > children[i].bound()){
                    children[i] = null;
                    expansions++;
                }
                */
            }
            for (int i = 0; i < children.length; i++) {
                if (children[i] != null && children[i].distance() == best_dist && targetBoard == null) {
                    targetBoard = children[i];
                    children[i] = null;
                    break;
                }
            }
            sendBoard(children);
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


    /**
     * Client wait server to sent message
     * @throws IOException
     */
    private void waitingMessage() throws IOException {
        synchronized (this) {
            while (waitingMessage) {
                try {
                    wait();
                } catch (Exception e) {
                    // ignored
                }

            }
        }
    }

    /**
     * client wait till server messages ready
     * @throws IOException
     */
    private void waitingServer() throws IOException {
        synchronized (this) {
            while (waitingServer) {
                System.err.println("Waiting Server");
                try {
                    wait();
                } catch (Exception e) {
                    // ignored
                }
            }
        }
    }


    synchronized void messageReady(){
        waitingMessage = false;
        notifyAll();
    }

    synchronized void serverReady() throws IOException {
        waitingServer = false;
        notifyAll();
        System.err.println("Server Ready! ");
    }

    public void setFinished() throws IOException {
        waitingMessage = false;
        running = false;

        // Close send port.
        sendPort.close();
        System.err.println("Sender closed");

        // Close receive port.
        receiver.close();
        System.err.println("Receiver closed");

    }

    /**
     * Receive port connect to server
     * @throws Exception
     */
    private void receiverConnect() throws Exception{
        // Create a receive port, pass ourselves as the message upcall
        // handler
        receiver = ibis.createReceivePort(Ida.ONE2MANY, "fromServer",this);
        // enable connections
        receiver.enableConnections();
        // enable upcalls
        receiver.enableMessageUpcalls();

    }

    /**
     * Send port connect to Server
     * @throws Exception
     */
    private void senderConnect() throws Exception{
        sendPort = ibis.createSendPort(Ida.MANY2ONE);
        sendPort.connect(serverId, "toServer");
    }


}
