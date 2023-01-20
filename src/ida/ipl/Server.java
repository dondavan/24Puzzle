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

    // Set to 1 when result found
    private int result = 0;

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

        run(board,bound);
        ibis.end();

    }

    private void run(Board board,int bound) throws IOException {
        generateJob(board,bound);
        serverReady();

        while (result == 0){

        }
        // Close receive port.
        receivePort.close();
        System.err.println("receiver closed");
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
                ReceivePortIdentifier clientPortId = sendPort.connect(identifier, "fromServer");
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

    private void sendMessage(IbisIdentifier target) throws IOException {
        // If still have job pending and reuslt not found,
        // Get board and send job to client
        if(!jobCache.isEmpty() && result == 0){
            Board board = jobCache.get(jobCache.size()-1);
            jobCache.remove(jobCache.size()-1);
            ArrayList byteBoard = new ArrayList<>();
            byteBoard.add(1);
            for (SendPort sendPort :sendPorts){
                if((sendPort.connectedTo())[0].ibisIdentifier().equals(target)){
                    WriteMessage w = sendPort.newMessage();
                    w.writeObject(byteBoard);
                    w.finish();
                    System.err.println("Bytes: " + w.bytesWritten());
                    System.err.println("Send to: " + target);
                }
            }
        }else {
            for (SendPort sendPort :sendPorts){
                // Close ports.
                sendPort.close();
            }
            setFinished();
        }
    }


    private void serverReady() throws IOException {
        for (SendPort sendPort :sendPorts){
            ArrayList byteBoard = new ArrayList<>();
            byteBoard.add(1);
            byteBoard.add(2);
            WriteMessage w = sendPort.newMessage();
            w.writeObject(byteBoard);
            System.err.println(w.bytesWritten());
            w.finish();
            System.err.println("Notified  " + (sendPort.connectedTo())[0].ibisIdentifier());
        }
    }

    /**
     * Override upcall to implement messageupcall
     * Automatic triggered when new message incoming
     * @param message
     * @throws IOException
     */
    @Override
    public void upcall(ReadMessage message) throws IOException {
        int s = message.readInt();
        IbisIdentifier identifier = message.origin().ibisIdentifier();
        message.finish();
        System.err.println("Received from: " + identifier + " Content : " + s);
        sendMessage(identifier);
    }

    synchronized void wakeUp(){
        notifyAll();
    }

    synchronized void setFinished() {
        finished = true;
        result = 1;
    }
}
