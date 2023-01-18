package ida.ipl;

import ibis.ipl.*;

import java.io.IOException;
import java.util.ArrayList;

public class Ibis implements MessageUpcall, RegistryEventHandler{

    /**
     *  Ibis properties
     **/


    PortType portType = new PortType(PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_DATA, PortType.RECEIVE_AUTO_UPCALLS,
            PortType.CONNECTION_MANY_TO_MANY);

    IbisCapabilities ibisCapabilities = new IbisCapabilities(
            IbisCapabilities.CLOSED_WORLD,
            IbisCapabilities.ELECTIONS_STRICT,
            IbisCapabilities.MEMBERSHIP_TOTALLY_ORDERED);

    /** Set to true when server received message. */
    boolean finished = false;

    public void upcall(ReadMessage message) throws IOException {
        String s = message.readString();
        System.err.println("Received string: " + s);
        setFinished();
    }

    synchronized void setFinished() {
        finished = true;
        notifyAll();
    }

    public void joined(IbisIdentifier joinedIbis) { System.err.println(joinedIbis);
    }
    public void died(IbisIdentifier corpse) {
        System.err.println(corpse);
    }

    public void gotSignal(String signal, IbisIdentifier source) {

    }

    public void left(IbisIdentifier leftIbis) {
        System.err.println(leftIbis);
    }

    public void electionResult(String electionName, IbisIdentifier winner) { // NOTHING
    }
    public void poolClosed() { // NOTHING
    }
    public void poolTerminated(IbisIdentifier source) { // NOTHING
    }

    /**
     * Ibis server, responsible for distributing board branch to client
     * @param myIbis
     * @throws IOException
     */
    private void server(ibis.ipl.Ibis myIbis) throws IOException {

        // Create a receive port, pass ourselves as the message upcall
        // handler
        ReceivePort receiver = myIbis.createReceivePort(portType, "server",
                this);
        // enable connections
        receiver.enableConnections();
        // enable upcalls
        receiver.enableMessageUpcalls();

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
    /**
     * Ibis client, receive work from server and explore board
     * @param myIbis
     * @param server
     * @throws IOException
     */
    private void client(ibis.ipl.Ibis myIbis, IbisIdentifier server) throws IOException {

        // Create a send port for sending requests and connect.
        SendPort sender = myIbis.createSendPort(portType);
        sender.connect(server, "server");

        // Send the message.
        WriteMessage w = sender.newMessage();
        w.writeString("Hi there");
        w.finish();

        // Close ports.
        sender.close();
    }

    /**
     * Instantiate and run client, server.
     * @throws Exception
     */
    public void run() throws Exception {
        // Create an ibis instance.
        ibis.ipl.Ibis ibis = IbisFactory.createIbis(ibisCapabilities, null, portType);
        // Elect a server
        IbisIdentifier server = ibis.registry().elect("Server");
        ibis.registry().waitUntilPoolClosed();
        IbisIdentifier[] joinedIbises = ibis.registry().joinedIbises();
        //IbisIdentifier[] as = ibis.registry().joinedIbises();
        // If I am the server, run server, else run client.
        if (server.equals(ibis.identifier())) {
            server(ibis);
        } else {
            client(ibis, server);
        }
        for(IbisIdentifier ibisIdentifier:joinedIbises)System.err.println(ibisIdentifier);
        // End ibis.
        ibis.end();
    }

}
