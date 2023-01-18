package ida.ipl;

import ibis.ipl.*;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Ibis server, responsible for distributing board branch to client
 */
public class Server {

    /**
     *  Ibis properties
     **/

    PortType portType = new PortType(
            PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_DATA,
            PortType.RECEIVE_AUTO_UPCALLS,
            PortType.CONNECTION_ONE_TO_MANY);

    private ibis.ipl.Ibis ibis;
    private ArrayList<SendPort> sendPorts;

    public Server(ibis.ipl.Ibis ibis) throws Exception{

        // Create an ibis instance.
        this.ibis = ibis;
        sendPorts = new ArrayList<SendPort>();
        ibis.registry().waitUntilPoolClosed();
        IbisIdentifier[] joinedIbises = ibis.registry().joinedIbises();
        System.err.println("Server "+ibis.identifier());
        senderConnect(joinedIbises);
        System.err.println("Sending Msg");
        sendMessage();
        System.err.println("Msg sent");
        ibis.end();
    }

    private void senderConnect(IbisIdentifier[] ibisIdentifiers) throws Exception {
        for(IbisIdentifier identifier:ibisIdentifiers){
            if(!identifier.equals(ibis.identifier())){
                SendPort sendPort = ibis.createSendPort(portType);
                sendPort.connect(identifier, "fromServer");
                System.err.println(sendPort.name() + "  "  + identifier);
                sendPorts.add(sendPort);
            }
        }
    }

    private void sendMessage() throws IOException {
        // Send the message.
        for (SendPort sendPort :sendPorts){
            WriteMessage w = sendPort.newMessage();
            w.writeString("Hi there");
            w.finish();

            // Close ports.
            sendPort.close();
        }
    }

}
