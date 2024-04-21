public class Passage {

    private String hour;
    private String originRoom;
    private String destinationRoom;

    public Passage(String hour, String originRoom, String destinationRoom) {
        this.hour = hour;
        this.originRoom = originRoom;
        this.destinationRoom = destinationRoom;
    }

    public String getHour() {
        return hour;
    }
    
    public String getOriginRoom() {
        return originRoom;
    }
    
    public String getDestinationRoom() {
        return destinationRoom;
    }
    
}
