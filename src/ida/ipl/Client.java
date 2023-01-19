package ida.ipl;

import ibis.ipl.*;

import java.io.IOException;


/**
 * Ibis client, receive work from server and explore board
 */
public class Client implements MessageUpcall{

    /**
     *  Ibis properties
     **/


    private ibis.ipl.Ibis ibis;
    private boolean finished = false;
    private ReceivePort receiver;
    static int expansions;
    IbisIdentifier serverId;
    SendPort sendPort;

    public Client(ibis.ipl.Ibis ibis,IbisIdentifier serverId) throws Exception {

        // Create an ibis instance.
        this.ibis = ibis;
        this.serverId = serverId;
        ibis.registry().waitUntilPoolClosed();

        System.err.println("CLient "+ibis.identifier());

        // send port to server
        senderConnect();
        receiverConnect();
        receiveMessage();

        WriteMessage w = sendPort.newMessage();
        w.writeInt(8);
        w.finish();
        sendPort.close();
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

    public void upcall(ReadMessage message) throws IOException {
        int s = message.readInt();
        System.err.println("Received string: " + s);
        setFinished();
    }

    synchronized void setFinished() {
        finished = true;
        notifyAll();
    }

    private void receiveMessage() throws IOException {
        synchronized (this) {
            while (!finished) {
                try {
                    wait();
                } catch (Exception e) {
                    // ignored
                }
            }
        }

        // Close receive port.
        receiver.close();
        System.err.println("receiver closed");
    }

    /**
     * expands this board into all possible positions, and returns the number of
     * solutions. Will cut off at the bound set in the board.
     */
    private static int solutions(Board board, BoardCache cache) {
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
    private static int solutions(Board board) {
        expansions++;
        if (board.distance() == 0) {
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


    private static void solve(Board board, boolean useCache) {
        BoardCache cache = null;
        if (useCache) {
            cache = new BoardCache();
        }
        int bound = board.distance();
        int solutions;

        System.out.println("Try bound ");
        System.out.flush();

        do {
            board.setBound(bound);

            System.out.print(bound + " ");
            System.out.flush();

            expansions = 0;
            if (useCache) {
                solutions = solutions(board, cache);
            } else {
                solutions = solutions(board);
            }

            bound += 2;
            System.err.println("Expansions: " + expansions);
        } while (solutions == 0);

        System.out.println("\nresult is " + solutions + " solutions of "
                + board.bound() + " steps");

    }

}
