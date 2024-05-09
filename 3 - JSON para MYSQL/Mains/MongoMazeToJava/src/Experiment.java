public class Experiment {

    private int ID;
    private Maze maze;
    private int maxWaitTime;
    private int gapRatos;

    public Experiment(int ID, Maze maze, int maxWaitTime, int gapRatos) {
        this.ID = ID;
        this.maze = maze;
        this.maxWaitTime = maxWaitTime;
        this.gapRatos = gapRatos;
    }

    public int getID() {
        return ID;
    }

    public Maze getMaze() {
        return maze;
    }

    public int getMaxWaitTime() {
        return maxWaitTime;
    }

    public int getGapRatos() {
        return gapRatos;
    }
}
