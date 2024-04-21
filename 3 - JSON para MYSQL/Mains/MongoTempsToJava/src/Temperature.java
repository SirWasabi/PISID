public class Temperature {

    private String hora;
    private String reading;
    private String sensor;

    public Temperature(String hora, String reading, String sensor) {
        this.hora = hora;
        this.reading = reading;
        this.sensor = sensor;
    }

    public String getHora() {
        return hora;
    }
    
    public String getReading() {
        return reading;
    }
    
    public String getSensor() {
        return sensor;
    }
    
}
