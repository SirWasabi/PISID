public class Room {

    private int roomID;
    private int population = 0;

    public Room(int roomID) {
        this.roomID = roomID;
    }

    public int getRoomID() {
        return roomID;
    }

    public int getPopulation() {
        return population;
    }

    public void setPopulation(int population) {
        this.population = population;
    }

    public boolean isRoomFull(int max) {
        if (population > max) {
            return true;
        }
        return false;
    }
    
}
