package ida.sequential;

final class Ida {

    static int expansions;

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

		System.out.print("Try bound ");
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

	public static void main(String[] args) {
		String fileName = null;
		boolean cache = true;
        String saveFile = null;

		/* Use suitable default value. */
		int length = 103;
		//int length = 104;

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
				System.err
						.println("could not initialize board from file: " + e);
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

		System.out.println("Running IDA*, initial board:");
		System.out.println(initialBoard);

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
