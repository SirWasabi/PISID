
public class Sensor {

    private int ID;
    private Temperature lastTemperature;

    public Sensor(int ID) {
        this.ID = ID;
    }

    public int getID() {
        return ID;
    }

    public Temperature getLastTemperature() {
        return lastTemperature;
    }

    public void setLastTemperature(Temperature lastTemperature) {
        this.lastTemperature = lastTemperature;
    }
}
