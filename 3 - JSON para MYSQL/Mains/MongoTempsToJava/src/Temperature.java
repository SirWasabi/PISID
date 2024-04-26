import java.time.LocalDateTime;

public class Temperature {

    private LocalDateTime hour;
    private double reading;
    private int sensor;

    public Temperature(LocalDateTime hour, double reading, int sensor) {
        this.hour = hour;
        this.reading = reading;
        this.sensor = sensor;
    }

    public LocalDateTime getHour() {
        return hour;
    }

    public double getReading() {
        return reading;
    }

    public int getSensor() {
        return sensor;
    }

    public void setHour(LocalDateTime hour) {
        this.hour = hour;
    }

    public void setReading(double reading) {
        this.reading = reading;
    }

    public void setSensor(int sensor) {
        this.sensor = sensor;
    }
    
}
