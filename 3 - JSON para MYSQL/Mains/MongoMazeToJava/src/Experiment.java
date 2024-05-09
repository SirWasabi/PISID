public class Experiment {

    private int ID;
    private Maze maze;
    private int maxWaitTime;

    public Experiment(int ID, Maze maze, int maxWaitTime) {
        this.ID = ID;
        this.maze = maze;
        this.maxWaitTime = maxWaitTime;
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
}
