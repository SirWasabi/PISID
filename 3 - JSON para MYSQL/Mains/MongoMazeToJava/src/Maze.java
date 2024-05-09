import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

public class Maze {

    private Connection conn_maze;
    private String sql_maze_database_connection_to;
    private String sql_maze_database_user_to;
    private String sql_maze_database_password_to;
    private String sql_maze_table_config;
    private String sql_maze_table_layout;

    private double temperatureDefault;
    private int number_of_rooms;
    private int room_max;
    private int number_of_rats;
    private ArrayList<Crossing> crossings = new ArrayList<>();
    private ArrayList<Room> rooms = new ArrayList<>();

    public Maze(String sql_maze_database_connection_to, String sql_maze_database_user_to,
            String sql_maze_database_password_to, String sql_maze_table_config, String sql_maze_table_layout, int room_max, int number_of_rats) {
        this.sql_maze_database_connection_to = sql_maze_database_connection_to;
        this.sql_maze_database_password_to = sql_maze_database_password_to;
        this.sql_maze_database_user_to = sql_maze_database_user_to;
        this.sql_maze_table_config = sql_maze_table_config;
        this.sql_maze_table_layout = sql_maze_table_layout;
        this.room_max = room_max;
        this.number_of_rats = number_of_rats;

        connectMazeMySQL();
        getMazeInfo();
        getMazeConfig();
        setupRooms(number_of_rooms);
    }

    public Connection getConn_maze() {
        return conn_maze;
    }

    public ArrayList<Crossing> getCrossings() {
        return crossings;
    }

    public ArrayList<Room> getRooms() {
        return rooms;
    }

    public int getNumber_of_rooms() {
        return number_of_rooms;
    }

    public double getTemperatureDefault() {
        return temperatureDefault;
    }

    public int getRoom_max() {
        return room_max;
    }

    public int getNumber_of_rats() {
        return number_of_rats;
    }

    public void connectMazeMySQL() {
        System.out.println("Getting maze \n");
        try {
            Class.forName("org.mariadb.jdbc.Driver");
            conn_maze = DriverManager.getConnection(sql_maze_database_connection_to, sql_maze_database_user_to,
                    sql_maze_database_password_to);
        } catch (Exception e) {
            System.out.println("Mysql Server Destination down, unable to make the connection. " + e);
        }
    }

    public void getMazeConfig() {
        String command = "Select * FROM " + sql_maze_table_config;
        try {
            PreparedStatement statement = conn_maze.prepareStatement(command);
            ResultSet result = statement.executeQuery();

            while (result.next()) {
                temperatureDefault = Double.parseDouble(result.getString("temperaturaprogramada"));
                number_of_rooms = Integer.parseInt(result.getString("numerodesalas"));

                System.out.println(
                        "Temperatura Programada: " + temperatureDefault + ", Numero de Salas: " + number_of_rooms);
            }

            result.close();
            statement.close();
        } catch (Exception e) {
            System.out.println("Error Selecting from the database . " + e);
            System.out.println(command);
        }
    }

    public void getMazeInfo() {
        String command = "Select * FROM " + sql_maze_table_layout;
        try {
            PreparedStatement statement = conn_maze.prepareStatement(command);
            ResultSet result = statement.executeQuery();

            while (result.next()) {
                String roomA = result.getString("salaa");
                String roomB = result.getString("salab");

                crossings.add(new Crossing(Integer.parseInt(roomA), Integer.parseInt(roomB)));
            }

            result.close();
            statement.close();
        } catch (Exception e) {
            System.out.println("Error Selecting from the database . " + e);
            System.out.println(command);
        }
    }

    public void setupRooms(int numberOfRooms) {
        for (int i = 0; i < numberOfRooms; i++) {
            rooms.add(new Room(i + 1));
        }

        rooms.get(0).setPopulation(number_of_rats);

        for (Room r : rooms) {
            System.out.println(r.getRoomID() + " - " + r.getPopulation());
        }
    }

    public boolean existsCrossing(int originRoom, int destinationRoom) {
        for (Crossing crossing : crossings) {
            if (crossing.getOriginRoom() == originRoom && crossing.getDestinationRoom() == destinationRoom) {
                return true;
            }
        }
        return false;
    }

    public boolean checkRoomMax(int destinationRoom) {
        for(Room room : rooms) {
            if(room.getRoomID() == destinationRoom) {
                if(room.isRoomFull(room_max)) {
                    return true;
                }
            }
        }
        return false;
    }

    public Crossing getCrossing(int originRoom, int destinationRoom) {
        for(Crossing crossing : crossings) {
            if (crossing.getOriginRoom() == originRoom && crossing.getDestinationRoom() == destinationRoom) {
                return crossing;
            }
        }
        return null;
    }

    public Room getRoom(int room_id) {
        for(Room room : rooms) {
            if(room.getRoomID() == room_id) {
                return room;
            }
        }
        return null;
    }

}
