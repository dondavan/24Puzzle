package ida.ipl;

import ibis.ipl.*;

import java.io.IOException;
import java.util.ArrayList;
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
    static int CUT_OFF_DEPTH = 4;
    static int SERVER_READY = 3;
    static int RESULT_FOUND = 1;
    static int RESULT_NOT_FOUND = 0;


    /**
     *  Ibis properties
     **/
    private ibis.ipl.Ibis ibis;


    static int QUEUE_SIZE = 999999;
    private ArrayList<SendPort> sendPorts;
    private ArrayBlockingQueue<Board> jobQueue;
    private ReceivePort receivePort;
    private Board initialBoard;
    private boolean clientComputing;

    // Set to 1 when result found
    private int result = 0;
    private int pendingCoomputing = 0;
    private int bound;
    static int expansions;


    // Timer
    long start;
    long end;

    /**
     *
     *
     *      Todo  Waiting message
     *      can not handle duplicate message
     *
     *
     *
     *
     *
     *
     *
     *
     *
     * @param ibis
     * @param initial
     * @throws Exception
     */
    public Server(ibis.ipl.Ibis ibis, Board initial) throws Exception{

        // Create an ibis instance.
        this.ibis = ibis;
        sendPorts = new ArrayList<SendPort>();
        jobQueue = new ArrayBlockingQueue<Board>(QUEUE_SIZE);
        ibis.registry().waitUntilPoolClosed();
        IbisIdentifier[] joinedIbises = ibis.registry().joinedIbises();

        this.initialBoard = initial;

        receiverConnect();
        senderConnect(joinedIbises);

        run();
        setFinished();

        ibis.end();
        end = System.currentTimeMillis();
        System.err.println("ida took " + (end - start) + " milliseconds");
    }

    private void run() throws IOException {
        bound = initialBoard.distance();
        serverReady();
        start = System.currentTimeMillis();

        System.err.print("Try bound ");

        while (result == 0) {
            initialBoard.setBound(bound);
            jobQueue.add(initialBoard);
            clientComputing = true;

            System.err.print(bound + " ");

            expansions = 0;
            // Let client expand board
            synchronized (this) {
                while (clientComputing) {
                    try {
                        wait();
                    } catch (Exception e) {
                        // ignored
                    }
                }
            }
            bound += 2;
            System.err.println("Expansions: " + expansions);
        }
        System.err.println("\nresult is " + result + " solutions of "
                + initialBoard.bound() + " steps");
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

    private void sendMessage(IbisIdentifier target,int flag) throws IOException {
        // If still have job pending and reuslt not found,
        if(flag == SEND_BOARD){
            synchronized (this) {
                while (clientComputing && jobQueue.isEmpty() ) {
                    try {
                        wait();
                    } catch (Exception e) {
                        // ignored
                    }
                }
            }

            if(!jobQueue.isEmpty() && result == 0){
                Board board = jobQueue.poll();
                //System.err.println("Queue Job Depth: " + board.depth());
                byte[] byteBoard = new byte[NSQRT*NSQRT + 4];
                byte[] byteBuffer = board.getByteBoard();
                for(int i =0;i<byteBuffer.length;i++){
                    byteBoard[i] = byteBuffer[i];
                }
                Integer intPrevX = new Integer(board.getPrevX());
                Integer intPrevY = new Integer(board.getPrevY());
                Integer intBound = new Integer(board.bound());
                Integer intDepth = new Integer(board.depth());
                byteBoard[NSQRT * NSQRT] = intPrevX.byteValue();
                byteBoard[NSQRT * NSQRT + 1] = intPrevY.byteValue();
                byteBoard[NSQRT * NSQRT + 2] = intBound.byteValue();
                byteBoard[NSQRT * NSQRT + 3] = intDepth.byteValue();

                for (SendPort sendPort :sendPorts){
                    if((sendPort.connectedTo())[0].ibisIdentifier().equals(target)){
                        WriteMessage w = sendPort.newMessage();
                        w.writeArray(byteBoard);
                        w.finish();
                        pendingCoomputing ++;
                        w.bytesWritten();
                        //System.err.println("Sent: + Depth "+ byteBoard[NSQRT * NSQRT + 3]);
                        //System.err.println("Send to: " + target + "Pending: "+ pendingCoomputing);
                    }
                }

            }
        } else {
            return;
        }

    }


    private void serverReady() throws IOException {
        for (SendPort sendPort :sendPorts){
            byte[] byteBoard = new byte[1];
            Integer intServer = new Integer(SERVER_READY);
            byteBoard[0] = intServer.byteValue();
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
        int flag = clientMessage[clientMessage.length-1];

        // Client send children board
        if(clientMessage[clientMessage.length-1] == RECV_BOARD){
            byte[] byteBoard = new byte[NSQRT*NSQRT + 4];
            int count = clientMessage[clientMessage.length-2];
            //System.err.println("Count: "+ count);
            for(int i = 0; i < count; i++ ){
                for(int j = 0; j < NSQRT*NSQRT + 4 ; j++){
                    byteBoard[j] = clientMessage[(NSQRT*NSQRT + 4) * i +j];
                }
                Board board = new Board(byteBoard);

                jobQueue.add(board);
            }
        }
        // Result Found
        else if(clientMessage[clientMessage.length-1] == RESULT_FOUND){
            result = clientMessage[clientMessage.length-1];
            setClientComputing();
        }
        //  Client result not found
        else if(clientMessage[clientMessage.length-1] == RESULT_NOT_FOUND){
            pendingCoomputing --;
            //System.err.println("Pending:  "+pendingCoomputing);
            if (jobQueue.isEmpty() && pendingCoomputing == 0)setClientComputing();
        }
        IbisIdentifier identifier = message.origin().ibisIdentifier();
        message.finish();
        sendMessage(identifier,flag);
    }

    synchronized void setClientComputing(){
        clientComputing = false;
        notifyAll();
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
}
