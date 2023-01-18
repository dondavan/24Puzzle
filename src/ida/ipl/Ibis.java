package ida.ipl;

import ibis.ipl.*;

import java.io.IOException;
import java.util.ArrayList;

public class Ibis{

    /**
     *  Ibis properties
     **/

    PortType portType = new PortType(
            PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_DATA,
            PortType.RECEIVE_AUTO_UPCALLS,
            PortType.CONNECTION_ONE_TO_MANY);

    IbisCapabilities ibisCapabilities = new IbisCapabilities(
            IbisCapabilities.CLOSED_WORLD,
            IbisCapabilities.ELECTIONS_STRICT,
            IbisCapabilities.MEMBERSHIP_TOTALLY_ORDERED);

    /**
     * Instantiate and run client, server.
     * @throws Exception
     */
    public void run() throws Exception {
        // Create an ibis instance.
        ibis.ipl.Ibis ibis = IbisFactory.createIbis(ibisCapabilities, null, portType);
        ibis.registry().waitUntilPoolClosed();
        // Elect a server
        IbisIdentifier serverId = ibis.registry().elect("Server");
        // If I am the server, run server, else run client.
        if (serverId.equals(ibis.identifier())) {
            Server server = new Server(ibis);
        } else {
            Client client = new Client(ibis);
        }
    }

}
