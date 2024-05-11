public class Experiment {

    private int ID;
    private Maze maze;
    private int maxWaitTime;
    private int gapRatos;
    private int intermediate_interval;

    public Experiment(int ID, Maze maze, int maxWaitTime, int gapRatos, int intermediate_interval) {
        this.ID = ID;
        this.maze = maze;
        this.maxWaitTime = maxWaitTime;
        this.gapRatos = gapRatos;
        this.intermediate_interval = intermediate_interval;
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

    public int getIntermediate_interval() {
        return intermediate_interval;
    }
}
