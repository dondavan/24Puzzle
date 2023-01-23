package ida.ipl;

import com.sun.jmx.remote.internal.ArrayQueue;
import ibis.ipl.*;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;

import static ida.ipl.Board.NSQRT;

/**
 * Ibis server, responsible for distributing board branch to client
 */
public class Server implements MessageUpcall{

    /**
     *  Ibis properties
     **/
    private ibis.ipl.Ibis ibis;


    static int QUEUE_SIZE = 999999;
    private boolean finished = false;
    private ArrayList<SendPort> sendPorts;
    private ArrayBlockingQueue<Board> jobQueue;
    private ReceivePort receivePort;
    private Board initialBoard;
    private boolean clientComputing;

    // Set to 1 when result found
    private int result = 0;

    private int bound;

    public Server(ibis.ipl.Ibis ibis, Board initial) throws Exception{

        // Create an ibis instance.
        this.ibis = ibis;
        sendPorts = new ArrayList<SendPort>();
        jobQueue = new ArrayBlockingQueue<Board>(QUEUE_SIZE);
        ibis.registry().waitUntilPoolClosed();
        IbisIdentifier[] joinedIbises = ibis.registry().joinedIbises();
        System.err.println("Server "+ibis.identifier());

        this.initialBoard = initial;

        receiverConnect();
        senderConnect(joinedIbises);

        run();
        ibis.end();

    }

    private void run() throws IOException {
        bound = initialBoard.distance() + 6;
        serverReady();
        while (result == 0) {
            initialBoard.setBound(bound);
            jobQueue.add(initialBoard);
            clientComputing = true;
            // Let client expand board
            synchronized (this) {
                while (clientComputing) {
                    System.err.println("clientComputing");
                    try {
                        wait();
                    } catch (Exception e) {
                        // ignored
                    }
                }
                System.err.println("clientComputing Done");
            }
            bound += 2;
        }
        // Close receive port.
        receivePort.close();
        System.err.println("receiver closed");
    }

    /*
    synchronized int generateJob(Board board,int bound){
        int solutions = 0;
        board.setBound(bound);
        System.err.println("Generating job with bound : "+ bound);
        solutions = expancdSolution(board,jobCache);
        notifyAll();
        return solutions;
    }
*/
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

        synchronized (this) {
            while (clientComputing && jobQueue.isEmpty() ) {
                System.err.println("Empty waiting");
                try {
                    wait();
                } catch (Exception e) {
                    // ignored
                }
            }
        }

        if(!jobQueue.isEmpty() && result == 0){
            Board board = jobQueue.poll();
            //System.err.println("Size: " + jobQueue.size());
            //System.err.println(board.bound());
            byte[] byteBoard = new byte[NSQRT*NSQRT + 3];
            byte[] byteBuffer = board.getByteBoard();
            for(int i =0;i<byteBuffer.length;i++){
                byteBoard[i] = byteBuffer[i];
            }
            Integer intPrevX = new Integer(board.getPrevX());
            Integer intPrevY = new Integer(board.getPrevY());
            Integer intBound = new Integer(bound);
            byteBoard[NSQRT * NSQRT] = intPrevX.byteValue();
            byteBoard[NSQRT * NSQRT + 1] = intPrevY.byteValue();
            byteBoard[NSQRT * NSQRT + 2] = intBound.byteValue();

            for (SendPort sendPort :sendPorts){
                if((sendPort.connectedTo())[0].ibisIdentifier().equals(target)){
                    WriteMessage w = sendPort.newMessage();
                    w.writeArray(byteBoard);
                    w.finish();
                    w.bytesWritten();
                    //System.err.println("Send to: " + target);
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
            //System.err.println("Notified  " + (sendPort.connectedTo())[0].ibisIdentifier());
        }
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

        // Request for board
        if(clientMessage[clientMessage.length-1] == 6){

        }
        // Client send children board
        else if(clientMessage[clientMessage.length-1] == 5){
            byte[] byteBoard = new byte[NSQRT*NSQRT + 3];
            int count = clientMessage[clientMessage.length-2];
            //System.err.println("Count: "+ count);
            for(int i = 0; i < count; i++ ){
                for(int j = 0; j < NSQRT*NSQRT + 3 ; j++){
                    byteBoard[j] = clientMessage[(NSQRT*NSQRT + 3) * i +j];
                }
                Board board = new Board(byteBoard);
                /*
                System.err.println(board);
                System.err.println("Bound: "+ board.bound());
                System.err.println("X: "+ board.getPrevX());
                System.err.println("Y: "+ board.getPrevY());
                 */
                jobQueue.add(board);
            }
        }
        // Result Found
        else if(clientMessage[clientMessage.length-1] == 1){
            System.err.println("Result Found");
            result = clientMessage[clientMessage.length-1];
            if (jobQueue.isEmpty())setClientComputing();
        }
        //
        else if(clientMessage[clientMessage.length-1] == 0){
            result = clientMessage[clientMessage.length-1];
        }
        IbisIdentifier identifier = message.origin().ibisIdentifier();
        message.finish();
        sendMessage(identifier);
    }

    synchronized void setClientComputing(){
        clientComputing = false;
        notifyAll();
    }

    synchronized void setFinished() {
        finished = true;
        result = 1;
    }
}
