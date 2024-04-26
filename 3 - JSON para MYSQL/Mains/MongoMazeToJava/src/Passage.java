import java.time.LocalDateTime;

public class Passage {

    private LocalDateTime hour;
    private int originRoom;
    private int destinationRoom;

    public Passage(LocalDateTime hour, int originRoom, int destinationRoom) {
        this.hour = hour;
        this.originRoom = originRoom;
        this.destinationRoom = destinationRoom;
    }

    public LocalDateTime getHour() {
        return hour;
    }
    
    public int getOriginRoom() {
        return originRoom;
    }
    
    public int getDestinationRoom() {
        return destinationRoom;
    }

    public void setHour(LocalDateTime hour) {
        this.hour = hour;
    }

    public void setOriginRoom(int originRoom) {
        this.originRoom = originRoom;
    }

    public void setDestinationRoom(int destinationRoom) {
        this.destinationRoom = destinationRoom;
    }
    
}
