import java.util.ArrayList;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoMazeToJava {

    static MongoClient mongoClient;
    static MongoDatabase db;
    static MongoCollection<Document> mongocol;
    static String mongo_user = new String();
    static String mongo_password = new String();
    static String mongo_address = new String();
    static String mongo_host = new String();
    static String mongo_replica = new String();
    static String mongo_database = new String();
    static String mongo_collection = new String();
    static String mongo_authentication = new String();
    private static final int LARGE_TIMEOUT_MS = 24 * 60 * 60 * 1000; // 24 hours

    static Connection conn_sql;
    static String sql_database_connection_to = new String();
    static String sql_database_password_to = new String();
    static String sql_database_user_to = new String();
    static String sql_table_to = new String();
    static String sql_table_alert = new String();

    static Connection conn_maze;
    static String sql_maze_database_connection_to = new String();
    static String sql_maze_database_password_to = new String();
    static String sql_maze_database_user_to = new String();
    static String sql_maze_table_config = new String();
    static String sql_maze_table_layout = new String();

    static ObjectId lastObjectId = null;
    static long frequency;
    final String[] sql_columns = { "Hora", "SalaOrigem", "SalaDestino" };
    final String[] sql_columns_alert = { "Sala", "SalaOrigem", "SalaDestino", "TipoAlerta", "Mensagem", "HoraEscrita" };
    final DateTimeFormatter date_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
    ArrayList<Crossing> crossings = new ArrayList<>();
    ArrayList<Room> rooms = new ArrayList<>();
    int room_max;
    int number_of_rats;
    String temperatureDefault;
    String number_of_rooms;

    public static void main(String[] args) {
        MongoMazeToJava conn = new MongoMazeToJava();

        conn.loadProperties();
        conn.connectMazeMySQL();
        conn.getMazeConfig();
        conn.getMazeInfo();
        conn.requestWithTimer();
    }

    public void loadProperties() {
        try {
            Properties p = new Properties();
            p.load(new FileInputStream("src/MongoMazeToJava.ini"));
            mongo_address = p.getProperty("mongo_address");
            mongo_user = p.getProperty("mongo_user");
            mongo_password = p.getProperty("mongo_password");
            mongo_replica = p.getProperty("mongo_replica");
            mongo_host = p.getProperty("mongo_host");
            mongo_database = p.getProperty("mongo_database");
            mongo_authentication = p.getProperty("mongo_authentication");
            mongo_collection = p.getProperty("mongo_collection");

            sql_table_to = p.getProperty("sql_table_to");
            sql_table_alert = p.getProperty("sql_table_alert");
            sql_database_connection_to = p.getProperty("sql_database_connection_to");
            sql_database_password_to = p.getProperty("sql_database_password_to");
            sql_database_user_to = p.getProperty("sql_database_user_to");

            sql_maze_table_config = p.getProperty("sql_maze_table_config");
            sql_maze_table_layout = p.getProperty("sql_maze_table_layout");
            sql_maze_database_connection_to = p.getProperty("sql_maze_database_connection_to");
            sql_maze_database_password_to = p.getProperty("sql_maze_database_password_to");
            sql_maze_database_user_to = p.getProperty("sql_maze_database_user_to");

            frequency = Long.parseLong(p.getProperty("frequency"));
        } catch (Exception e) {
            System.out.println("Error reading MongoMazeToJava.ini file " + e);
        }
    }

    public boolean connectMongo() {
        String mongoURI = new String();
        mongoURI = "mongodb://";
        if (mongo_authentication.equals("true"))
            mongoURI = mongoURI + mongo_user + ":" + mongo_password + "@";
        mongoURI = mongoURI + mongo_address;
        if (!mongo_replica.equals("false"))
            if (mongo_authentication.equals("true"))
                mongoURI = mongoURI + "/?replicaSet=" + mongo_replica + "&authSource=admin";
            else
                mongoURI = mongoURI + "/?replicaSet=" + mongo_replica;
        else if (mongo_authentication.equals("true"))
            mongoURI = mongoURI + "/?authSource=admin";

        mongoURI = mongoURI + "&serverSelectionTimeoutMS=" + LARGE_TIMEOUT_MS;

        try {
            if (mongoClient == null) {
                mongoClient = new MongoClient(new MongoClientURI(mongoURI));
            } else {
                mongoClient.listDatabaseNames().first();
            }
            db = mongoClient.getDatabase(mongo_database);
            mongocol = db.getCollection(mongo_collection);
            System.out.println("MONGO CONNECTED");
            return true;
        } catch (Exception e) {
            System.out.println("MONGO NOT CONNECTED");
            return false;
        }
    }

    public boolean connectMySQL() {
        try {
            if (conn_sql == null || conn_sql.isClosed()) {
                Class.forName("org.mariadb.jdbc.Driver");
                conn_sql = DriverManager.getConnection(sql_database_connection_to, sql_database_user_to,
                        sql_database_password_to);
                return true;
            }
        } catch (SQLException | ClassNotFoundException e) {
            System.out.println("MYSQL SERVER IS NOT CONNECTED");
            return false;
        }

        return true;
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

    private void getMazeConfig() {
        String command = "Select * FROM " + sql_maze_table_config;
        try {
            PreparedStatement statement = conn_maze.prepareStatement(command);
            ResultSet result = statement.executeQuery();

            while (result.next()) {
                temperatureDefault = result.getString("temperaturaprogramada");
                number_of_rooms = result.getString("numerodesalas");

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

    private void getMazeInfo() {
        String command = "Select * FROM " + sql_maze_table_layout;
        try {
            PreparedStatement statement = conn_maze.prepareStatement(command);
            ResultSet result = statement.executeQuery();

            while (result.next()) {
                String roomA = result.getString("salaa");
                String roomB = result.getString("salab");

                addCrossingToArray(roomA, roomB); // TODO Check se dá faulty data
            }

            result.close();
            statement.close();
        } catch (Exception e) {
            System.out.println("Error Selecting from the database . " + e);
            System.out.println(command);
        }
    }

    private boolean isTestingActive() {
        String command = "SELECT * FROM experiencia WHERE Estado='Ativo';";
        try {
            PreparedStatement statement = conn_sql.prepareStatement(command);
            ResultSet result = statement.executeQuery();

            while (result.next()) {
                room_max = result.getInt("LimiteRatos");
                number_of_rats = result.getInt("NumeroRatos");
                System.out.println("EXPERIENCIA ATIVA - NUMERO MAX:" + room_max);
                return true;
            }

            result.close();
            statement.close();
        } catch (Exception e) {
            System.out.println("Error Selecting from the database . " + e);
            System.out.println(command);
        }

        return false;
    }

    public void requestWithTimer() {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                FindIterable<Document> docs = null;

                if (connectMongo() && connectMySQL()) {
                    if (lastObjectId == null) {
                        try {
                            docs = mongocol.find();
                        } catch (NullPointerException e) {
                            System.out.println("No data yet on MongoDB");
                        }
                    } else {
                        docs = mongocol.find(new Document("_id", new Document("$gt", lastObjectId)));
                    }

                    if (docs != null) {
                        processData(docs);
                    }
                } else {
                    System.out.println("ERROR CONNECTING");
                }
            }
        }, 0, frequency);
    }

    public void deleteOldMongoDocs() {
        mongocol.deleteMany(new Document("_id", new Document("$lte", lastObjectId)));
    }

    public void processData(FindIterable<Document> docs) {
        boolean wrotePassageToSQL = false;
        for (Document doc : docs) {
            String hour_string = (String) doc.get("Hora");
            String originRoom_string = String.valueOf((Integer) doc.get("SalaOrigem"));
            String destinationRoom_string = String.valueOf((Integer) doc.get("SalaDestino"));

            // if (isTestingActive()) {
            //     if(rooms.isEmpty()) {
            //         createRooms(Integer.parseInt(number_of_rooms));
            //     }
                if (!containsFaultyData(hour_string, originRoom_string, destinationRoom_string)) {
                    LocalDateTime hour = LocalDateTime.parse(hour_string, date_formatter);
                    int originRoom = Integer.valueOf(originRoom_string);
                    int destinationRoom = Integer.valueOf(destinationRoom_string);

                    Passage passage = new Passage(hour, originRoom, destinationRoom);

                    if (checkRoomMax(passage)) {
                        // writeAlertToSQL(hour_string, destinationRoom_string, "CRITICO", "ROOM MAX");
                    }

                    if (checkInactivity(passage)) {
                        // writeAlertToSQL(hour_string, destinationRoom_string, "CRITICO", "CRITICO -
                        // INATIVIDADE");
                    }

                    //updateRooms();

                    wrotePassageToSQL = writePassageToSQL(passage);

                } else {
                    Alert alert = new Alert(null, originRoom_string, destinationRoom_string, null, null, "AVARIA",
                            "Avaria - WRONG DATA", hour_string);
                    writeAlertToSQL(alert);
                }
            //}
            if (wrotePassageToSQL)
                lastObjectId = (ObjectId) doc.get("_id");
        }

        if (wrotePassageToSQL)
            deleteOldMongoDocs();
    }

    private void updateRooms() {
        System.out.println("Updating Rooms");
    }

    private boolean containsFaultyData(String hour, String originRoom, String destinationRoom) {
        try {
            LocalDateTime.parse(hour, date_formatter);
            Integer.parseInt(originRoom);
            Integer.parseInt(destinationRoom);
        } catch (Exception e) {
            return true;
        }

        // if (checkCrossings(crossings, Integer.parseInt(originRoom),
        // Integer.parseInt(destinationRoom))) {
        // return true;
        // }

        return false;
    }

    private boolean checkCrossings(ArrayList<Crossing> crossings, int originRoom, int destinationRoom) {
        for (Crossing crossing : crossings) {
            if (crossing.isSameCrossing(originRoom, destinationRoom))
                return true;
        }

        return false;
    }

    private void createRooms(int numberOfRooms) {
        for (int i = 0; i < numberOfRooms; i++) {
            rooms.add(new Room(i + 1));
        }

        for (Room r : rooms) {
            System.out.println(r.getRoomID() + " - " + r.getPopulation());
        }
    }

    private void addCrossingToArray(String roomA, String roomB) {
        crossings.add(new Crossing(Integer.parseInt(roomA), Integer.parseInt(roomB)));
    }

    private boolean checkInactivity(Passage passage) {
        // System.out.println("Check Inactive");
        return false;
    }

    private boolean checkRoomMax(Passage passage) {
        // System.out.println("Check Max Num");
        return false;
    }

    private String SQLColumnsToString(String[] columns) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < columns.length; i++) {
            result.append(columns[i]);
            if (i < columns.length - 1) {
                result.append(", ");
            }
        }
        return result.toString();
    }

    private boolean writePassageToSQL(Passage passage) {
        String command = "Insert into " + sql_table_to + " (" + SQLColumnsToString(sql_columns) + ") values (?, ?, ?);";
        try {
            PreparedStatement statement = conn_sql.prepareStatement(command);
            statement.setObject(1, passage.getHour());
            statement.setInt(2, passage.getOriginRoom());
            statement.setInt(3, passage.getDestinationRoom());
            System.out.println(statement.toString());
            if (!conn_sql.isClosed()) {
                int result = statement.executeUpdate();
                statement.close();
                return true;
            } else {
                System.out.println("ITEM NOT INSERTED BECAUSE MYSQL CONNECTION IS CLOSED");
                return false;
            }
        } catch (Exception e) {
            System.out.println("ERROR INSERTING IN DATABASE - " + e);
            System.out.println(command);
            return false;
        }
    }

    private boolean writeAlertToSQL(Alert alert) {
        String command = "Insert into " + sql_table_alert + " (" + SQLColumnsToString(sql_columns_alert)
                + ") values (?, ?, ?, ?, ?, ?);";
        try {
            PreparedStatement statement = conn_sql.prepareStatement(command);
            statement.setString(1, alert.getRoom());
            statement.setString(2, alert.getOriginRoom());
            statement.setString(3, alert.getDestinationRoom());
            statement.setString(4, alert.getType());
            statement.setString(5, alert.getMessage());
            statement.setString(6, alert.getHour());
            System.out.println(statement.toString());
            if (!conn_sql.isClosed()) {
                int result = statement.executeUpdate();
                statement.close();
                return true;
            } else {
                System.out.println("ITEM NOT INSERTED BECAUSE MYSQL CONNECTION IS CLOSED");
                return false;
            }
        } catch (Exception e) {
            System.out.println("ERROR INSERTING IN DATABASE - " + e);
            System.out.println(command);
            return false;
        }
    }

}
