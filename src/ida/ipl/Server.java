package ida.ipl;

import ibis.ipl.*;

import java.io.IOException;

/**
 * Ibis server, responsible for distributing board branch to client
 */
public class Server implements MessageUpcall {

    /**
     *  Ibis properties
     **/

    PortType portType = new PortType(PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_DATA, PortType.RECEIVE_AUTO_UPCALLS,
            PortType.CONNECTION_ONE_TO_MANY);

    private ibis.ipl.Ibis ibis;
    private boolean finished = false;
    private SendPort sender;

    public Server(ibis.ipl.Ibis ibis) throws Exception{

        // Create an ibis instance.
        this.ibis = ibis;
        ibis.registry().waitUntilPoolClosed();
        IbisIdentifier[] joinedIbises = ibis.registry().joinedIbises();

        System.err.println(ibis.identifier());
        senderConnect(joinedIbises);
        System.err.println(ibis.identifier());
        System.err.println("Sending Msg");
        sendMessage();
        System.err.println("Msg sent");
        ibis.end();
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

    private void senderConnect(IbisIdentifier[] ibisIdentifiers) throws Exception {
        sender = ibis.createSendPort(portType);
        for(IbisIdentifier identifier:ibisIdentifiers){
            System.err.println(identifier);
            sender.connect(identifier, "fromServer");
            System.err.println(sender.name());
        }
    }

    private void sendMessage() throws IOException {
        // Send the message.
        WriteMessage w = sender.newMessage();
        w.writeString("Hi there");
        w.finish();

        // Close ports.
        sender.close();
    }

}
