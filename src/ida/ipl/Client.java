package ida.ipl;

import ibis.ipl.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;


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
    private boolean waitingMessage = false;
    private boolean waitingServer = true;
    private byte[] byteBoard;
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
            waitingMessage();
            sendMessage(solveResult);
        }
        setFinished();
    }

    private int solve(Board board, boolean useCache) {
        BoardCache cache = null;
        if (useCache) {
            cache = new BoardCache();
        }
        int solutions;
        System.err.println("Board bound: "+ board.bound());

        expansions = 0;
        if (useCache) {
            solutions = solutions(board, cache);
        } else {
            solutions = solutions(board);
        }
        System.err.println("Result is " + solutions +" Expansions: " + expansions);
        return solutions;

    }

    public void upcall(ReadMessage message) throws IOException, ClassNotFoundException {
        System.err.println("Receievced from +" + message.origin().ibisIdentifier());
        byte[] byteBoard = (byte[]) message.readObject();
        if(waitingServer){
            // Ignore first message ready message from server
            serverReady();
        }else {
            Board board = new Board(byteBoard);
            //System.err.println(board);
            solveResult = solve(board,true);
            messageReady();
        }
        message.finish();
    }


    /**
     * send pending children board to server
     */
    private void sendBoard(Board[] boards) throws IOException {
        byte[] bytes = new byte[1];
        bytes[0] = 1;
        WriteMessage w = sendPort.newMessage();
        w.writeArray(bytes);
        w.finish();

        System.err.println("Send to Server " + bytes[0]);
        waitingMessage = true;
    }

    /**
     * send message to notify server this client is ready
     */
    private void sendMessage(int result) throws IOException {
        byte[] bytes = new byte[1];
        bytes[0] = 6;
        WriteMessage w = sendPort.newMessage();
        w.writeArray(bytes);
        w.finish();
        System.err.println("Send to Server " + solveResult);
        waitingMessage = true;
    }

    /**
     * expands this board into all possible positions, and returns the number of
     * solutions. Will cut off at the bound set in the board.
     */
    private int solutions(Board board, BoardCache cache) {
        expansions++;
        if (board.distance() == 0) {
            System.err.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>> Gotcha!");
            System.err.println(board);
            return 1;
        }

        if (board.distance() > board.bound()) {
            return 0;
        }

        Board[] children = board.makeMoves(cache);
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
    private int solutions(Board board) {
        expansions++;
        if (board.distance() == 0) {
            System.err.println(board);
            return 1;
        }

        if (board.distance() > board.bound()) {
            return 0;
        }

        Board[] children = board.makeMoves();
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
                System.err.println("Waiting Message");
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

    synchronized void serverReady(){
        waitingServer = false;
        notifyAll();
        System.err.println("Server Ready! ");
    }

    synchronized void setFinished() throws IOException {
        // Close receive port.
        receiver.close();
        System.err.println("Receiver closed");
        // Close send port.
        sendPort.close();
        System.err.println("Sender closed");

        finished = true;
        notifyAll();
    }

}
