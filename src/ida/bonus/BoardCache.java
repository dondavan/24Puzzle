package ida.bonus;


import ida.ipl.Board;

public class BoardCache {

    public static final int MAX_CACHE_SIZE = 10 * 1024;

    int size;
    ida.ipl.Board[] cache;

    public BoardCache() {
        size = 0;
        cache = new ida.ipl.Board[MAX_CACHE_SIZE];
    }

    public ida.ipl.Board get(ida.ipl.Board original) {
        if (size > 0) {
            size--;
            ida.ipl.Board result = cache[size];
            result.init(original);
            return result;
        } else {
            return new ida.ipl.Board(original);
        }
    }

    public void put(ida.ipl.Board[] boards) {
        for (Board board: boards) {
            if (board == null) {
                return;
            }
            if (size >= MAX_CACHE_SIZE) {
                return;
            }
            cache[size] = board;
            size++;
        }
    }
}
