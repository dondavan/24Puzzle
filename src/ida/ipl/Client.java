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
    private Ibis ibis;
    ReceivePort receiver;
    static int expansions;
    IbisIdentifier serverId;
    SendPort sendPort;

    // Set to false when result found
    private boolean running = true;


    // Coordiante client status
    private boolean waitingMessage = true;
    private boolean waitingServer = true;
    private boolean useCache;
    private int solveResult;


    private ArrayBlockingQueue<Board> jobQueue;     // Job queue

    long total = 0;
    public Client(Ibis ibis, IbisIdentifier serverId, boolean useCache) throws Exception {

        jobQueue = new ArrayBlockingQueue<Board>(QUEUE_SIZE);

        // Assign an ibis instance.
        this.ibis = ibis;
        this.serverId = serverId;
        ibis.registry().waitUntilPoolClosed();


        // Connect to server
        senderConnect();
        receiverConnect();

        this.useCache = useCache;

        run();
        ibis.end();
    }

    private void run() throws IOException {
        waitingServer();
        while (running){
            sendMessage(SEND_BOARD,SEND_BOARD,SEND_BOARD);
            waitingMessage();
            if(!jobQueue.isEmpty()){
                Board board = jobQueue.poll();

                long start = System.nanoTime();
                solveResult = solve(board,useCache);
                long end = System.nanoTime();
                total += (end-start);
                // if result found, sendboard inside solve() will be called
                if(solveResult !=0 ) sendMessage(solveResult,expansions,RESULT_FOUND);
                else sendMessage(solveResult,expansions,RESULT_NOT_FOUND);

            }
        }
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
        return solutions;

    }

    /**
     * expands this board into all possible positions, and returns the number of
     * solutions. Will cut off at the bound set in the board.
     */
    private int solutions(Board board, BoardCache cache) throws IOException {
        expansions++;

        if (board.distance() == 0) {
            Board[] result = new Board[1];
            result[0] = board;
            sendBoard(result,RESULT_BOARD);
            return 1;
        }

        if (board.distance() > board.bound()) {
            return 0;
        }

        Board[] children = board.makeMoves(cache, board.depth());
        int result = 0;

        for (int i = 0; i < children.length; i++) {
            if (children[i] != null) {
                result += solutions(children[i], cache);
            }
        }

        cache.put(children);
        return result;
    }

    /**
     * expands this board into all possible positions, and returns the number of
     * solutions. Will cut off at the bound set in the board.
     */
    private int solutions(Board board) throws IOException {
        expansions++;
        if (board.distance() == 0) {
            Board[] result = new Board[1];
            result[0] = board;
            sendBoard(result,RESULT_BOARD);
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



    public void upcall(ReadMessage message) throws IOException, ClassNotFoundException {
        byte[] bytes = (byte[]) message.readObject();

        if(bytes[bytes.length-1] == END){
            setFinished();
        }else if(bytes[bytes.length-1] == SERVER_READY){
            serverReady();
        }else if(bytes[bytes.length-1] == SEND_BOARD){
            Board newboard = new Board(bytes);
            jobQueue.add(newboard);
            messageReady();
        }

        message.finish();
    }


    /**
     * send pending children board to server
     */
    private synchronized void sendBoard(Board[] boards,int flag) throws IOException {
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
                int intPrevX = boards[i].getPrevX();
                bytes[(NSQRT*NSQRT + 4)* count + (NSQRT*NSQRT)] = (byte) intPrevX;
                int intPrevY = count;
                bytes[(NSQRT*NSQRT + 4)* count + (NSQRT*NSQRT) +1 ] = (byte) intPrevY;
                int intBound = boards[i].bound();
                bytes[(NSQRT*NSQRT + 4)* count + (NSQRT*NSQRT) +2 ] = (byte) intBound;
                int intDepth = boards[i].depth();
                bytes[(NSQRT*NSQRT + 4)* count + (NSQRT*NSQRT) +3 ] = (byte) intDepth;
            }
            count++;
        }
        if(count != 0){
            bytes[ (NSQRT * NSQRT + 4) * 4] = (byte) count;
            bytes[ bytes.length-1] = (byte) flag;

            WriteMessage w = sendPort.newMessage();
            w.writeArray(bytes);
            w.finish();

        }
        waitingMessage = true;
    }

    /**
     * send message to notify server this client is ready
     */
    private synchronized void sendMessage(int result, int expansion,int flag) throws IOException {
        // result + expansion + flag
        byte[] bytes = new byte[24];

        ByteBuffer buf = ByteBuffer.wrap(bytes);
        Integer flagInt = flag;
        byte flagByte = flagInt.byteValue();
        buf.putInt(result);
        buf.putInt(8,expansion);
        buf.put(bytes.length-1,flagByte);

        if (running) {
            WriteMessage w = sendPort.newMessage();
            w.writeArray(bytes);
            w.finish();
            waitingMessage = true;
        }
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

    synchronized void serverReady()  {
        waitingServer = false;
        notifyAll();
    }

    synchronized public void setFinished() throws IOException {
        waitingMessage = false;
        running = false;

        // Close send port.
        sendPort.close();
        notifyAll();
    }

    /**
     * Receive port connect to server
     * @throws Exception
     */
    private void receiverConnect() throws Exception{
        // Create a receive port, pass ourselves as the message upcall
        // handler
        receiver = ibis.createReceivePort(Ida.ONE2ONE, "fromServer",this);
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
