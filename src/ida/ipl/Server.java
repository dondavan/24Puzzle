package ida.ipl;

import ibis.ipl.*;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Ibis server, responsible for distributing board branch to client
 */
public class Server implements MessageUpcall{

    /**
     *  Ibis properties
     **/


    private ibis.ipl.Ibis ibis;
    private boolean finished = false;
    private ArrayList<SendPort> sendPorts;
    private ArrayList<Board> jobCache;
    private ReceivePort receivePort;

    public Server(ibis.ipl.Ibis ibis, Board board,int bound) throws Exception{

        // Create an ibis instance.
        this.ibis = ibis;
        sendPorts = new ArrayList<SendPort>();
        jobCache = new ArrayList<Board>();
        ibis.registry().waitUntilPoolClosed();
        IbisIdentifier[] joinedIbises = ibis.registry().joinedIbises();

        System.err.println("Server "+ibis.identifier());
        receiverConnect();
        senderConnect(joinedIbises);
        generateJob(board,bound);
        System.err.println("Joblength + " + jobCache.size());
        sendMessage();
        receiveMessage();
        ibis.end();
    }

    private int generateJob(Board board,int bound){
        int solutions = 0;
        board.setBound(bound);
        System.out.print("Generating job with bound : "+ bound);
        System.out.flush();
        solutions = expancdSolution(board,jobCache);
        return solutions;
    }

    private int expancdSolution(Board board,ArrayList<Board> jobCache) {
        if (board.distance() == 0) {
            System.err.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>> Gotcha!");
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
                result += expancdSolution(children[i], jobCache);
            }
        }
        for(Board sibling:children){
            if (sibling != null){
                jobCache.add(sibling);
            }
        }
        return result;
    }

    private void senderConnect(IbisIdentifier[] ibisIdentifiers) throws Exception {
        for(IbisIdentifier identifier:ibisIdentifiers){
            if(!identifier.equals(ibis.identifier())){
                SendPort sendPort = ibis.createSendPort(Ida.ONE2MANY);
                sendPort.connect(identifier, "fromServer");
                sendPorts.add(sendPort);
            }
        }
    }

    private void receiverConnect() throws IOException {
        receivePort = ibis.createReceivePort(Ida.MANY2ONE, "toServer",this);
        // enable connections
        receivePort.enableConnections();
        // enable upcalls
        receivePort.enableMessageUpcalls();
    }


    private void sendMessage() throws IOException {
        // Send the message.
        for (SendPort sendPort :sendPorts){
            WriteMessage w = sendPort.newMessage();
            w.writeInt(jobCache.size());
            w.finish();

            // Close ports.
            sendPort.close();
        }
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
        receivePort.close();
        System.err.println("receiver closed");
    }

    @Override
    public void upcall(ReadMessage message) throws IOException {
        int s = message.readInt();
        System.err.println("Received string: " + s);
        setFinished();
    }

    synchronized void setFinished() {
        finished = true;
        notifyAll();
    }
}
