package ida.ipl;


import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;

import java.util.ArrayList;
import java.util.List;

final class Ida {

    static int expansions;

    static PortType ONE2MANY = new PortType(
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


    /**
     * expands this board into all possible positions, and returns the number of
     * solutions. Will cut off at the bound set in the board.
     */
    private static int solutions(Board board, BoardCache cache) {
        expansions++;
        if (board.distance() == 0) {
            System.err.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>> Gotcha!");
            System.err.println(board);
            return 1;
        }

        if (board.distance() > board.bound()) {
            return 0;
        }

        Board[] children = board.makeMoves(cache);
        int result = 0;

        for (int i = 0; i < children.length; i++) {
            if (children[i] != null) {
                result += solutions(children[i], cache);
            }
        }
        cache.put(children);
        return result;
    }

    /**
     * expands this board into all possible positions, and returns the number of
     * solutions. Will cut off at the bound set in the board.
     */
    private static int solutions(Board board) {
        expansions++;
        if (board.distance() == 0) {
            return 1;
        }

        if (board.distance() > board.bound()) {
            return 0;
        }

        Board[] children = board.makeMoves();
        int result = 0;

        for (int i = 0; i < children.length; i++) {
            if (children[i] != null) {
                result += solutions(children[i]);
            }
        }
        return result;
    }

    private static void solve(Board board, boolean useCache) {
        BoardCache cache = null;
        if (useCache) {
            cache = new BoardCache();
        }
        int bound = board.distance();
        int solutions;

        System.out.println("Try bound ");
        System.out.flush();

        do {
            board.setBound(bound);

            System.out.print(bound + " ");
            System.out.flush();

            expansions = 0;
            if (useCache) {
                solutions = solutions(board, cache);
            } else {
                solutions = solutions(board);
            }

            bound += 2;
            System.err.println("Expansions: " + expansions);
        } while (solutions == 0);

        System.out.println("\nresult is " + solutions + " solutions of "
                + board.bound() + " steps");

    }

    public static void run(Board initial) throws Exception {
        // Create an ibis instance.
        ibis.ipl.Ibis ibis = IbisFactory.createIbis(ibisCapabilities, null,ONE2MANY,MANY2ONE);
        ibis.registry().waitUntilPoolClosed();
        // Elect a server
        IbisIdentifier serverId = ibis.registry().elect("Server");
        // If I am the server, run server, else run client.
        if (serverId.equals(ibis.identifier())) {
            Server server = new Server(ibis,initial);
        } else {
            Client client = new Client(ibis,serverId);
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
        try {
            run(initialBoard);
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }

        System.err.println("Running IDA*, initial board:");
        System.err.println(initialBoard);

        long start = System.currentTimeMillis();
        solve(initialBoard, cache);
        long end = System.currentTimeMillis();

        // NOTE: this is printed to standard error! The rest of the output
        // is
        // constant for each set of parameters. Printing this to standard
        // error
        // makes the output of standard out comparable with "diff"
        System.err.println("ida took " + (end - start) + " milliseconds");
    }

}
