import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

    static Connection conn_sql;
    static String sql_database_connection_to = new String();
    static String sql_database_password_to = new String();
    static String sql_database_user_to = new String();
    static String sql_table_to = new String();

    static Connection conn_maze;
    static String sql_maze_database_connection_to = new String();
    static String sql_maze_database_password_to = new String();
    static String sql_maze_database_user_to = new String();
    static String sql_maze_table_config = new String();
    static String sql_maze_table_layout = new String();

    static ObjectId lastObjectId = null;
    static long frequency;
    final String[] sql_columns = { "Hora", "SalaOrigem", "SalaDestino" };
    final DateTimeFormatter date_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
    ArrayList<Corredor> corredores = new ArrayList<>();
    ArrayList<Integer> rooms = new ArrayList<>();
    String temperaturaDefault;
    String numeroSalas;

    public static void main(String[] args) {
        MongoMazeToJava conn = new MongoMazeToJava();

        conn.loadProperties();
        conn.connectMongo();
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

    public void connectMongo() {
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

        MongoClient mongoClient = new MongoClient(new MongoClientURI(mongoURI));
        db = mongoClient.getDatabase(mongo_database);
        mongocol = db.getCollection(mongo_collection);
    }

    public void connectMySQL() {
        try {
            Class.forName("org.mariadb.jdbc.Driver");
            if (conn_sql == null || conn_sql.isClosed()) {
                conn_sql = DriverManager.getConnection(sql_database_connection_to, sql_database_user_to,
                        sql_database_password_to);
                System.out.println(conn_sql);
            } else {
                System.out.println("SQL CONNECTED ALREADY");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Mysql Server Destination down, unable to make the connection. " + e);
        }
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
                temperaturaDefault = result.getString("temperaturaprogramada");
                numeroSalas = result.getString("numerodesalas");

                System.out.println(
                        "Temperatura Programada: " + temperaturaDefault + ", Numero de Salas: " + numeroSalas);
            }

            setRoomArray(numeroSalas); // pode ser null

            result.close();
            statement.close();
        } catch (Exception e) {
            System.out.println("Error Inserting in the database . " + e);
            System.out.println(command);
        }
    }

    private void getMazeInfo() {
        String command = "Select * FROM " + sql_maze_table_layout;
        try {
            PreparedStatement statement = conn_maze.prepareStatement(command);
            ResultSet result = statement.executeQuery();

            while (result.next()) {
                String salaA = result.getString("salaa");
                String salaB = result.getString("salab");
                String centimetros = result.getString("centimetro");

                corredores.add(new Corredor(Integer.parseInt(salaA), Integer.parseInt(salaB))); //TODO Check se dá faulty data

                System.out.println("Sala A: " + salaA + ", Sala B: " + salaB + ", Centimetro: " + centimetros);
            }

            result.close();
            statement.close();
        } catch (Exception e) {
            System.out.println("Error Inserting in the database . " + e);
            System.out.println(command);
        }
    }

    public void requestWithTimer() {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                FindIterable<Document> docs = null;

                connectMySQL();
                if (conn_sql != null) {

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

                }
            }
        }, 0, frequency);
    }

    public void deleteOldMongoDocs() {
        System.out.println("BEFORE DELETE: " + lastObjectId + " - " + mongocol.count());
        mongocol.deleteMany(new Document("_id", new Document("$lte", lastObjectId)));
        System.out.println("AFTER DELETE: " + lastObjectId + " - " + mongocol.count() + "\n");
    }

    public void processData(FindIterable<Document> docs) {
        boolean wroteToSQL = false;
        for (Document doc : docs) {
            lastObjectId = (ObjectId) doc.get("_id");
            String hour_string = (String) doc.get("Hora");
            String originRoom_string = String.valueOf((Integer) doc.get("SalaOrigem"));
            String destinationRoom_string = String.valueOf((Integer) doc.get("SalaDestino"));
            if (!containsFaultyData(hour_string, originRoom_string, destinationRoom_string)) {
                LocalDateTime hour = LocalDateTime.parse(hour_string, date_formatter);
                int originRoom = Integer.valueOf(originRoom_string);
                int destinationRoom = Integer.valueOf(destinationRoom_string);

                Passage passage = new Passage(hour, originRoom, destinationRoom);

                if (checkRoomMax(passage)) {
                    writeAlertToSQL(hour_string, destinationRoom_string, "CRITICO", "ROOM MAX");
                }

                if (checkInactivity(passage)) {
                    writeAlertToSQL(hour_string, destinationRoom_string, "CRITICO", "CRITICO - INATIVIDADE");
                }

                wroteToSQL = writePassageToSQL(passage);
            } else {
                writeAlertToSQL(hour_string, destinationRoom_string, "Avaria", "Avaria - WRONG DATA");
            }
        }
        
        if(wroteToSQL) deleteOldMongoDocs();
    }

    private boolean containsFaultyData(String hour, String originRoom, String destinationRoom) {
        try {
            LocalDateTime.parse(hour, date_formatter);
            Integer.parseInt(originRoom);
            Integer.parseInt(destinationRoom);
        } catch (Exception e) {
            return true;
        }

        return checkCorredores(corredores, Integer.parseInt(originRoom), Integer.parseInt(destinationRoom));
    }

    private boolean checkCorredores(ArrayList<Corredor> corredores, int so, int sd) {
        for(Corredor c : corredores)
            if(c.corredorIgual(so, sd))
             return true;
        return false;
    }

    private void setRoomArray(String numeroSalas) {
        int numero = Integer.parseInt(numeroSalas);
        for(int i=0; i < numero; i++) {
            rooms.add(0);
        }
    }


    private boolean checkInactivity(Passage passage) {
        System.out.println("Check Inactive");
        return false;
    }

    private boolean checkRoomMax(Passage passage) {
        
        System.out.println("Check Max Num");
        return false;
    }

    private String SQLColumnsToString() {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < sql_columns.length; i++) {
            result.append(sql_columns[i]);
            if (i < sql_columns.length - 1) {
                result.append(", ");
            }
        }
        return result.toString();
    }

    private boolean writePassageToSQL(Passage passage) {
        String command = "Insert into " + sql_table_to + " (" + SQLColumnsToString() + ") values (?, ?, ?);";
        try {
            PreparedStatement statement = conn_sql.prepareStatement(command);
            statement.setObject(1, passage.getHour());
            statement.setInt(2, passage.getOriginRoom());
            statement.setInt(3, passage.getDestinationRoom());
            System.out.println(statement.toString());
            int result = statement.executeUpdate();
            statement.close();
            return true;
        } catch (Exception e) {
            System.out.println("Error Inserting in the database . " + e);
            System.out.println(command);
            return false;
        }
    }

    private void writeAlertToSQL(String hour, String room, String type, String message) {
        Alert alert = new Alert(hour, room, null, null, type, message);
        System.out.println("Writing " + alert.getMessage() + " to SQL");
    }

}
