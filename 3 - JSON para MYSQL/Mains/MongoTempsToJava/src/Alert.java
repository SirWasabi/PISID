public class Alert {

    private String hour;
    private String room;
    private String sensor;
    private String reading;
    private String type;
    private String message;

    public Alert(String hour, String room, String sensor, String reading, String type, String message) {
        this.hour = hour;
        this.room = room;
        this.sensor = sensor;
        this.reading = reading;
        this.type = type;
        this.message = message;
    }

    public String getHour() {
        return hour;
    }

    public String getRoom() {
        return room;
    }

    public String getSensor() {
        return sensor;
    }

    public String getReading() {
        return reading;
    }

    public String getType() {
        return type;
    }

    public String getMessage() {
        return message;
    }
}
