package ida.ipl;

import ibis.ipl.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
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
    static int CUT_OFF_DEPTH = 6;
    static int SERVER_READY = 3;
    static int RESULT_FOUND = 1;
    static int RESULT_NOT_FOUND = 0;


    /**
     *  Ibis properties
     **/
    private ibis.ipl.Ibis ibis;


    static int QUEUE_SIZE = 10000;
    private ArrayList<SendPort> sendPorts;          // Save send port to client
    private ArrayBlockingQueue<Board> jobQueue;     // Job queue
    private ReceivePort receivePort;
    private Board initialBoard;
    private boolean clientComputing;

    // Set to 1 when result found
    private int result = 0;
    private int pendingCoomputing = 0;
    private int bound;
    static int expansions;


    // Client Status Set
    int clientSize = 0;
    ArrayBlockingQueue<IbisIdentifier> waitingMessageSet;
    ArrayBlockingQueue<IbisIdentifier> computingSet;
    ArrayBlockingQueue<IbisIdentifier> doneSet;

    // Timer
    long start;
    long end;

    public Server(ibis.ipl.Ibis ibis, Board initial) throws Exception{

        waitingMessageSet = new ArrayBlockingQueue<IbisIdentifier>(10);
        computingSet = new ArrayBlockingQueue<IbisIdentifier>(10);
        doneSet = new ArrayBlockingQueue<IbisIdentifier>(10);

        // Assign ibis instance.
        this.ibis = ibis;
        sendPorts = new ArrayList<SendPort>();
        ibis.registry().waitUntilPoolClosed();
        IbisIdentifier[] joinedIbises = ibis.registry().joinedIbises();
        // Connnect to client
        receiverConnect();
        senderConnect(joinedIbises);

        this.initialBoard = initial;
        jobQueue = new ArrayBlockingQueue<Board>(QUEUE_SIZE);



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

            // Bound iteration finishes when job queue  is empty and all client done computing
            synchronized (this) {

                System.err.println("Before  Waiting "+ waitingMessageSet + " Computing "+ computingSet+ " Done "+ doneSet );
                System.err.println("Before  Jobs "+ jobQueue.size());

                while (!jobQueue.isEmpty() && result == 0) {

                    // Wait till client request for job
                    while(waitingMessageSet.isEmpty()){
                        try {
                            wait();
                        } catch (Exception e) {
                            // ignored
                        }
                    }

                    // Send job, if job Queue is not empty, and client waiting message
                    while(!jobQueue.isEmpty() && !waitingMessageSet.isEmpty()){
                        Iterator ID_iterator = waitingMessageSet.iterator();
                        while (ID_iterator.hasNext() && !jobQueue.isEmpty()) {
                            IbisIdentifier target = (IbisIdentifier) ID_iterator.next();
                            sendBoard(target);
                        }
                    }


                    // Wait till client finishes job
                    while(computingSet.size() != 0){
                        try {
                            wait();
                        } catch (Exception e) {
                            // ignored
                        }
                    }

                }
                System.err.println("After  Waiting "+ waitingMessageSet + " Computing "+ computingSet+ " Done "+ doneSet );
                System.err.println("After  Jobs "+ jobQueue.size());
            }

            bound += 2;
            System.err.println("Expansions: " + expansions);
        }
        System.err.println("\nresult is " + result + " solutions of "
                + initialBoard.bound() + " steps");
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
        IbisIdentifier identifier = message.origin().ibisIdentifier();
        //System.err.println("Recv from:" + identifier + "Flag: "+ flag);
        // Client send children board

        if(clientMessage[clientMessage.length-1] == SEND_BOARD){

            //System.err.println("===  Waiting "+ waitingMessageSet + " Computing "+ computingSet+ " Done "+ doneSet );
            done2Wait(identifier);
            wakeUp();
        }
        else if(clientMessage[clientMessage.length-1] == RECV_BOARD){
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
            comp2Done(identifier);
            result = clientMessage[clientMessage.length-1];
            setClientComputing();
            wakeUp();
        }
        //  Client result not found
        else if(clientMessage[clientMessage.length-1] == RESULT_NOT_FOUND){
            comp2Done(identifier);
            wakeUp();
            pendingCoomputing --;
            //System.err.println("Pending:  "+pendingCoomputing);
            if (jobQueue.isEmpty() && pendingCoomputing == 0)setClientComputing();
        }


        message.finish();
    }

    private synchronized void sendBoard(IbisIdentifier target) throws IOException {
        Board board = jobQueue.poll();
        //System.err.println("Queue Job Depth: " + board.depth());
        byte[] byteBoard = new byte[NSQRT*NSQRT + 5];
        byte[] byteBuffer = board.getByteBoard();
        for(int i =0;i<byteBuffer.length;i++){
            byteBoard[i] = byteBuffer[i];
        }
        Integer intPrevX = new Integer(board.getPrevX());
        Integer intPrevY = new Integer(board.getPrevY());
        Integer intBound = new Integer(board.bound());
        Integer intDepth = new Integer(board.depth());
        Integer intFlag = new Integer(SEND_BOARD);
        byteBoard[NSQRT * NSQRT] = intPrevX.byteValue();
        byteBoard[NSQRT * NSQRT + 1] = intPrevY.byteValue();
        byteBoard[NSQRT * NSQRT + 2] = intBound.byteValue();
        byteBoard[NSQRT * NSQRT + 3] = intDepth.byteValue();
        byteBoard[NSQRT * NSQRT + 4] = intFlag.byteValue();

        for (SendPort sendPort :sendPorts){
            if((sendPort.connectedTo())[0].ibisIdentifier().equals(target)){
                WriteMessage w = sendPort.newMessage();
                w.writeArray(byteBoard);
                w.finish();
                wait2Comp(target);
                System.err.println("Send board: "+byteBoard[NSQRT * NSQRT + 3] + "remaining: "+ jobQueue.size());
                pendingCoomputing ++;
            }
        }


    }


    /**
     * Move target from waiting set to computing set
     * @param target
     */
    synchronized void wait2Comp(IbisIdentifier target){
        waitingMessageSet.remove(target);
        computingSet.add(target);
    }

    /**
     * Move target from computing set to done set
     * @param target
     */
    synchronized void comp2Done(IbisIdentifier target){
        computingSet.remove(target);
        doneSet.add(target);
    }

    /**
     * Move target from waiting set to computing set
     * @param target
     */
    synchronized void done2Wait(IbisIdentifier target){
        doneSet.remove(target);
        waitingMessageSet.add(target);
    }

    synchronized void wakeUp(){
        notifyAll();
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

    private void senderConnect(IbisIdentifier[] ibisIdentifiers) throws Exception {
        for(IbisIdentifier identifier:ibisIdentifiers){
            if(!identifier.equals(ibis.identifier())){
                SendPort sendPort = ibis.createSendPort(Ida.ONE2MANY);
                ReceivePortIdentifier clientPortId = sendPort.connect(identifier, "fromServer");
                sendPorts.add(sendPort);
                clientSize++;
                System.err.println("Client Size: "+ clientSize);
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
}
