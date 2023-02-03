package ida.ipl;


import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;

import java.util.ArrayList;
import java.util.List;

final class Ida {

    static PortType ONE2ONE = new PortType(
            PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_OBJECT,
            PortType.RECEIVE_AUTO_UPCALLS,
            PortType.CONNECTION_ONE_TO_MANY);

    static PortType MANY2ONE = new PortType(
            PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_OBJECT,
            PortType.RECEIVE_AUTO_UPCALLS,
            PortType.CONNECTION_MANY_TO_ONE);

    static IbisCapabilities ibisCapabilities = new IbisCapabilities(
            IbisCapabilities.CLOSED_WORLD,
            IbisCapabilities.ELECTIONS_STRICT,
            IbisCapabilities.MEMBERSHIP_TOTALLY_ORDERED,
            IbisCapabilities.SIGNALS);


    public static void run(Board initial,boolean cache) throws Exception {
        // Create an ibis instance.
        ibis.ipl.Ibis ibis = IbisFactory.createIbis(ibisCapabilities, null,ONE2ONE,MANY2ONE);
        ibis.registry().waitUntilPoolClosed();
        // Elect a server
        IbisIdentifier serverId = ibis.registry().elect("Server");
        // If I am the server, run server, else run client.
        if (serverId.equals(ibis.identifier())) {
            Server server = new Server(ibis,initial,cache);
        } else {
            Client client = new Client(ibis,serverId,cache);
        }
    }

    public static void main(String[] args) {
        String fileName = null;
        boolean cache = true;
        String saveFile = null;

        /* Use suitable default value. */
        int length = 103;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--file")) {
                fileName = args[++i];
            } else if (args[i].equals("--nocache")) {
                cache = false;
            } else if (args[i].equals("--length")) {
                i++;
                length = Integer.parseInt(args[i]);
            } else if (args[i].equals("-o")) {
                i++;
                saveFile = args[i];
            } else {
                System.err.println("No such option: " + args[i]);
                System.exit(1);
            }
        }

        Board initialBoard = null;

        if (fileName == null) {
            initialBoard = new Board(length);
        } else {
            try {
                initialBoard = new Board(fileName);
            } catch (Exception e) {
                System.err.println("could not initialize board from file: " + e);
                System.exit(1);
            }
        }
        if (saveFile != null) {
            System.out.println("Save board to " + saveFile);
            try {
                initialBoard.save(saveFile);
            } catch (java.io.IOException e) {
                System.err.println("Cannot save board: " + e);
            }
            System.exit(0);
        }



        long start = System.currentTimeMillis();
        try {
            run(initialBoard,cache);
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
        long end = System.currentTimeMillis();

        // NOTE: this is printed to standard error! The rest of the output
        // is
        // constant for each set of parameters. Printing this to standard
        // error
        // makes the output of standard out comparable with "diff"
        //System.err.println("IPL ida took " + (end - start) + " milliseconds");
    }

}
