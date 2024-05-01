public class Crossing {

    private int originRoom;
    private int destinationRoom;

    public Crossing(int originRoom, int destinationRoom) {
        this.originRoom = originRoom;
        this.destinationRoom = destinationRoom;
    }

    public boolean isSameCrossing(int originRoom, int destinationRoom) {
        return (this.originRoom == originRoom && this.destinationRoom == destinationRoom);
    }

    public int getOriginRoom() {
        return this.originRoom;
    }

    public int getDestinationRoom() {
        return this.destinationRoom;
    }

}
