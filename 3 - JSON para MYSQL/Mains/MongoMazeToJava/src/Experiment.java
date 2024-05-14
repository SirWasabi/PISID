import java.time.LocalDateTime;

public class Experiment {

    private int ID;
    private Maze maze;
    private int maxWaitTime;
    private int gapRatos;
    private int intermediate_interval;
    private LocalDateTime startTime;

    public Experiment(int ID, Maze maze, int maxWaitTime, int gapRatos, int intermediate_interval, LocalDateTime startTime) {
        this.ID = ID;
        this.maze = maze;
        this.maxWaitTime = maxWaitTime;
        this.gapRatos = gapRatos;
        this.intermediate_interval = intermediate_interval;
        this.startTime = startTime;
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
 
    public LocalDateTime getStartTime() {
        return startTime;
    }
}
