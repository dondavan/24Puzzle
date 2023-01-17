package ida.ipl;

import ibis.ipl.*;

import java.io.IOException;
import java.util.ArrayList;

public class Ibis implements MessageUpcall{

    /**
     *  Ibis properties
     **/


    PortType portType = new PortType(PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_DATA, PortType.RECEIVE_AUTO_UPCALLS,
            PortType.CONNECTION_ONE_TO_MANY);

    IbisCapabilities ibisCapabilities = new IbisCapabilities(
            IbisCapabilities.ELECTIONS_STRICT);

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

    /**
     * Ibis server, responsible for distributing board branch to client
     * @param myIbis
     * @throws IOException
     */
    private void server(ibis.ipl.Ibis myIbis, SendPort sender) throws IOException {

        // Send the message.
        WriteMessage w = sender.newMessage();
        w.writeString("Hi there");
        w.finish();

        // Close ports.
        sender.close();
    }

    private void serverConnect(ibis.ipl.Ibis myIbis, IbisIdentifier client) throws IOException {

        // Create a send port for sending requests and connect.
        SendPort sender = myIbis.createSendPort(portType);
        sender.connect(client, "FromServer");
        
    }

    private ReceivePort clientInit(ibis.ipl.Ibis myIbis)  throws IOException {

        // Create a receive port and enable connections.
        ReceivePort receiver = myIbis.createReceivePort(portType, "FromServer",
                this);
        receiver.enableConnections();

        // enable upcalls
        receiver.enableMessageUpcalls();

        return  receiver;
    }

    /**
     * Ibis client, receive work from server and explore board
     * @param myIbis
     * @throws IOException
     */
    private void client(ibis.ipl.Ibis myIbis) throws IOException {

    // Create a receive port and enable connections.
        ReceivePort receiver = myIbis.createReceivePort(portType, "FromServer",
                this);
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
     * Instantiate and run client, server.
     * @throws Exception
     */
    public void run() throws Exception {
        // Create an ibis instance.
        ibis.ipl.Ibis ibis = IbisFactory.createIbis(ibisCapabilities, null, portType);

        // Elect a server
        IbisIdentifier server = ibis.registry().elect("Server");

        System.err.println("Server is " + server);
        ArrayList<ReceivePortIdentifier> receivePortIdentifiers = new ArrayList<ReceivePortIdentifier>();

        // If I am the server, run server, else run client.
        if (server.equals(ibis.identifier())) {
            server(ibis);
        } else {
            System.err.println(ibis.identifier());
            ReceivePort receivePort= clientInit(ibis);
            receivePortIdentifiers.add(receivePort.identifier());
            client(receivePort);
        }

        if (server.equals(ibis.identifier())) {
            server(ibis);
        }
        // End ibis.
        ibis.end();
    }

}
