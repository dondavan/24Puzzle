package ida.ipl;

import ibis.ipl.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;

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


    // Coordiante client status
    private boolean finished = false;
    private boolean waitingMessage = true;
    private boolean waitingServer = true;
    private byte[] byteBoard;
    private Board board;
    private int solveResult;

    public Client(ibis.ipl.Ibis ibis,IbisIdentifier serverId) throws Exception {

        // Create an ibis instance.
        this.ibis = ibis;
        this.serverId = serverId;
        this.byteBoard = new byte[25];
        ibis.registry().waitUntilPoolClosed();

        System.err.println("CLient "+ibis.identifier());

        // send port to server
        senderConnect();
        receiverConnect();

        run();

        ibis.end();
    }

    private void receiverConnect() throws Exception{
        // Create a receive port, pass ourselves as the message upcall
        // handler
        receiver = ibis.createReceivePort(Ida.ONE2MANY, "fromServer",this);
        // enable connections
        receiver.enableConnections();
        // enable upcalls
        receiver.enableMessageUpcalls();

    }

    private void senderConnect() throws Exception{
        sendPort = ibis.createSendPort(Ida.MANY2ONE);
        sendPort.connect(serverId, "toServer");
    }

    private void run() throws IOException {
        waitingServer();
        while (result == 0){
            sendMessage(SEND_BOARD);
            waitingMessage();
            // Double check for message result
            if(result == 0) {
                solveResult = solve(board,true);
                sendMessage(solveResult);
            }
        }
    }


    public void upcall(ReadMessage message) throws IOException, ClassNotFoundException {
        //System.err.println("Receievced from +" + message.origin().ibisIdentifier());
        byte[] bytes = (byte[]) message.readObject();
        if(bytes[bytes.length-1] == RESULT_FOUND){
            setFinished();
        }
        if(waitingServer){
            // Ignore first message ready message from server
            serverReady();
        }else {
            this.board = new Board(bytes);
            messageReady();
        }
        message.finish();
    }


    /**
     * send pending children board to server
     */
    private void sendBoard(Board[] boards) throws IOException {

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
            w.finish();

            //System.err.println("Board send to Server " + bytes[(NSQRT * NSQRT + 3) * 4]);
        }
        waitingMessage = true;
    }

    /**
     * send message to notify server this client is ready
     */
    private void sendMessage(int result) throws IOException {
        byte[] bytes = new byte[1];
        Integer resultInt = new Integer(result);
        byte resultByte = resultInt.byteValue();
        bytes[0] = resultByte;
        WriteMessage w = sendPort.newMessage();
        w.writeArray(bytes);
        w.finish();
        //System.err.println("Message send to Server " + solveResult);
        waitingMessage = true;
    }


    private int solve(Board board, boolean useCache) throws IOException {
        //System.err.println("Compute Depth:"+ board.depth());
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
        System.err.println("*******Result is " + solutions +" Expansions: " + expansions);
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

        //System.err.println("Depth: "+ board.depth() + " Dist: " + board.distance() + " Bound: " + board.bound());
        if (board.distance() > board.bound()) {
            return 0;
        }

        Board[] children = board.makeMoves(cache, board.depth());
        int result = 0;

        /*
        for (int i = 0; i < children.length; i++) {
            if (children[i] != null ) {
                System.err.println(children[i]);
                System.err.println("Bound: "+ children[i].bound());
                System.err.println("Dist: "+ children[i].distance());
                System.err.println("X: "+ children[i].getPrevX());
                System.err.println("Y: "+ children[i].getPrevY());
            }
        }*/
        if(board.depth() < CUT_OFF_DEPTH){

            Board targetBoard = null;
            int best_dist = board.bound();
            /*
            for (int i = 0; i < children.length; i++) {
                if (children[i] != null ) {
                    System.err.println(children[i]);
                    System.err.println("Bound: "+ children[i].bound());
                    System.err.println("Dist: "+ children[i].distance());
                    System.err.println("Depth: "+ children[i].depth());
                }
            }
             */
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
            /*
            System.err.println(targetBoard);
            System.err.println("Bound: "+targetBoard.bound());
            System.err.println("X: "+ targetBoard.getPrevX());
            System.err.println("Y: "+ targetBoard.getPrevY());
            */
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



    private void waitingMessage() throws IOException {
        synchronized (this) {
            while (waitingMessage) {
                //System.err.println("Waiting Message");
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

    synchronized void setFinished() throws IOException {
        waitingMessage = false;
        notifyAll();

        result = 1;

        // Close receive port.
        receiver.close();
        System.err.println("Receiver closed");
        // Close send port.
        sendPort.close();
        System.err.println("Sender closed");

        finished = true;
    }

}
