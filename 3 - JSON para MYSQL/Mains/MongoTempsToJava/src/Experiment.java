import java.util.ArrayList;

public class Experiment {

    private int ID;
    private double idealTemperature;
    private double maxTemperatureVariation;
    private double temperatureGap;
    private double outlierGap;
    private ArrayList<Sensor> sensors = new ArrayList<Sensor>();

    public Experiment(int ID, double idealTemperature, double maxTemperatureVariation, double temperatureGap, double outlierGap) {
        this.ID = ID;
        this.idealTemperature = idealTemperature;
        this.maxTemperatureVariation = maxTemperatureVariation;
        this.temperatureGap = temperatureGap;
        this.outlierGap = outlierGap;
    }

    public int getID() {
        return ID;
    }

    public double getIdealTemperature() {
        return idealTemperature;
    }

    public double getMaxTemperatureVariation() {
        return maxTemperatureVariation;
    }

    public double getTemperatureGap() {
        return temperatureGap;
    }

    public double getOutlierGap() {
        return outlierGap;
    }

    public ArrayList<Sensor> getSensors() {
        return sensors;
    }

    public boolean existsSensor(int id) {
        for (Sensor sensor : sensors) {
            if (sensor.getID() == id) {
                return true;
            }
        }
        return false;
    }

    public Sensor getSensor(int id) {
        for (Sensor sensor : sensors) {
            if (sensor.getID() == id) {
                return sensor;
            }
        }
        return null;
    }
}
