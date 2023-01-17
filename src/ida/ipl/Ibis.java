package ida.ipl;

import ibis.ipl.*;

import java.io.IOException;

public class Ibis {

    /**
     *  Ibis properties
     **/

    PortType portType = new PortType(PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_DATA, PortType.RECEIVE_EXPLICIT,
            PortType.CONNECTION_ONE_TO_ONE);

    IbisCapabilities ibisCapabilities = new IbisCapabilities(
            IbisCapabilities.ELECTIONS_STRICT);

    /**
     * Ibis server, responsible for distributing board branch to client
     * @param myIbis
     * @throws IOException
     */
    private void server(ibis.ipl.Ibis myIbis) throws IOException {

        // Create a receive port and enable connections.
        ReceivePort receiver = myIbis.createReceivePort(portType, "server");
        receiver.enableConnections();

        // Read the message.
        ReadMessage r = receiver.receive();
        String s = r.readString();
        r.finish();
        System.out.println("Server received: " + s);

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

        System.out.println("Server is " + server);

        // If I am the server, run server, else run client.
        if (server.equals(ibis.identifier())) {
            server(ibis);
        } else {
            client(ibis, server);
        }

        // End ibis.
        ibis.end();
    }

}
