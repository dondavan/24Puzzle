package ida.ipl;

import ibis.ipl.*;

import java.io.IOException;
import java.util.ArrayList;

import static ida.ipl.Board.NSQRT;

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
    private Board board;

    // Set to 1 when result found
    private int result = 0;

    private int bound;

    public Server(ibis.ipl.Ibis ibis, Board board) throws Exception{

        // Create an ibis instance.
        this.ibis = ibis;
        sendPorts = new ArrayList<SendPort>();
        jobCache = new ArrayList<Board>();
        this.board = board;

        ibis.registry().waitUntilPoolClosed();
        IbisIdentifier[] joinedIbises = ibis.registry().joinedIbises();

        System.err.println("Server "+ibis.identifier());
        receiverConnect();
        senderConnect(joinedIbises);

        run();
        ibis.end();

    }

    private void run() throws IOException {
        bound = board.distance() + 2;
        generateJob(board,bound);
        serverReady();
        while (result == 0) {

        }
        // Close receive port.
        receivePort.close();
        System.err.println("receiver closed");
    }

    synchronized int generateJob(Board board,int bound){
        int solutions = 0;
        board.setBound(bound);
        System.err.println("Generating job with bound : "+ bound);
        solutions = expancdSolution(board,jobCache);
        notifyAll();
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
        if(jobCache.size() == 0){
            bound += 2;
            generateJob(board,bound);
            synchronized (this) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        // If still have job pending and reuslt not found,
        // Get board and send job to client
        if(!jobCache.isEmpty() && result == 0){
            Board board = jobCache.get(jobCache.size()-1);
            jobCache.remove(jobCache.size()-1);
            System.err.println(jobCache.size());
            byte[] byteBoard = new byte[26];
            byte[] byteBuffer = board.getByteBoard();
            for(int i =0;i<byteBuffer.length;i++){
                byteBoard[i] = byteBuffer[i];
            }

            Integer a = new Integer(bound);
            byteBoard[NSQRT * NSQRT] = a.byteValue();

            System.err.println(byteBoard[NSQRT * NSQRT]);
            for (SendPort sendPort :sendPorts){
                if((sendPort.connectedTo())[0].ibisIdentifier().equals(target)){
                    WriteMessage w = sendPort.newMessage();
                    w.writeArray(byteBoard);
                    w.finish();
                    w.bytesWritten();
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
            byte[] byteBoard = new byte[1];
            byteBoard[0] = 1;
            WriteMessage w = sendPort.newMessage();
            w.writeArray(byteBoard);
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
        int clientResult = message.readInt();
        IbisIdentifier identifier = message.origin().ibisIdentifier();
        message.finish();
        result = clientResult;
        System.err.println("Received from: " + identifier + " Content : " + result);
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
