public class Alert {

    
    private String room;
    private String originRoom;
    private String destinationRoom;
    private String sensor;
    private String reading;
    private String type;
    private String message;
    private String hour;

    public Alert(String room, String originRoom, String destinationRoom, String sensor, String reading, String type, String message, String hour) {
        this.room = room;
        this.originRoom = originRoom;
        this.destinationRoom = destinationRoom;
        this.sensor = sensor;
        this.reading = reading;
        this.type = type;
        this.message = message;
        this.hour = hour;
    }


    public String getRoom() {
        return room;
    }

    public String getOriginRoom() {
        return originRoom;
    }

    public String getDestinationRoom() {
        return destinationRoom;
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
    
    public String getHour() {
        return hour;
    }
}
