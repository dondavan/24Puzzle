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

    PortType portType = new PortType(
            PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_DATA,
            PortType.RECEIVE_AUTO_UPCALLS,
            PortType.CONNECTION_ONE_TO_MANY);

    private ibis.ipl.Ibis ibis;
    private boolean finished = false;
    private ReceivePort receiver;

    public Client(ibis.ipl.Ibis ibis) throws Exception {

        // Create an ibis instance.
        this.ibis = ibis;
        ibis.registry().waitUntilPoolClosed();
        System.err.println("CLient "+ibis.identifier());
        receiverConnect();
        System.err.println("Waiting Msg");
        receiveMessage();
        System.err.println("Msg sent");
        ibis.end();
    }

    private void receiverConnect() throws Exception{
        // Create a receive port, pass ourselves as the message upcall
        // handler
        receiver = ibis.createReceivePort(portType, "fromServer",this);

        // enable connections
        receiver.enableConnections();

        // enable upcalls
        receiver.enableMessageUpcalls();
    }

    public void upcall(ReadMessage message) throws IOException {
        String s = message.readString();
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
    }
}
