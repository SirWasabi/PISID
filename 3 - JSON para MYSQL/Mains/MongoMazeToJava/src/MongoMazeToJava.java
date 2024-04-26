import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
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

    static Connection connTo;
    static String sql_database_connection_to = new String();
    static String sql_database_password_to = new String();
    static String sql_database_user_to = new String();
    static String sql_table_to = new String();

    static long frequency;
    final String[] sql_columns = { "Hora", "SalaOrigem", "SalaDestino" };
    final DateTimeFormatter date_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    public static void main(String[] args) {
        MongoMazeToJava conn = new MongoMazeToJava();

        conn.loadProperties();
        conn.connectMongo();
        conn.connectMySQL();
        conn.getMazeFromCloud();
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
            connTo = DriverManager.getConnection(sql_database_connection_to, sql_database_user_to,
                    sql_database_password_to);
        } catch (Exception e) {
            System.out.println("Mysql Server Destination down, unable to make the connection. " + e);
        }
    }

    public void getMazeFromCloud() {
        System.out.println("Getting maze \n");
    }

    public ObjectId lastObjectId = null;

    public void requestWithTimer() {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                FindIterable<Document> docs;

                if (lastObjectId == null) {
                    docs = mongocol.find();
                } else {
                    docs = mongocol.find(new Document("_id", new Document("$gt", lastObjectId)));
                }

                processData(docs);

                mongocol.deleteMany(new Document("_id", new Document("$lte", lastObjectId)));

                System.out.println(lastObjectId.toString() + " - " + mongocol.count() + "\n");
            }
        }, 0, frequency);
    }

    public void processData(FindIterable<Document> docs) {
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
                    writeAlertToSQL(hour_string, destinationRoom_string, hour_string, originRoom_string,
                            destinationRoom_string);
                }

                if (checkInactivity(passage)) {
                    writeAlertToSQL(hour_string, destinationRoom_string, hour_string, originRoom_string,
                            destinationRoom_string);
                }

                System.out
                        .println(lastObjectId + " - " + hour_string + " - " + originRoom + " - " + destinationRoom
                                + "\n");

                writePassageToSQL(passage);
            } else {
                writeAlertToSQL(hour_string, destinationRoom_string, hour_string, originRoom_string,
                            destinationRoom_string);
            }
        }
    }

    private boolean containsFaultyData(String hour, String originRoom, String destinationRoom) {
        try {
            LocalDateTime.parse(hour, date_formatter);
            Integer.parseInt(originRoom);
            Integer.parseInt(destinationRoom);
        } catch (Exception e) {
            return true;
        }

        return false;
    }

    private boolean checkInactivity(Passage passage) {
        System.out.println("Check Inactive");
        return false;
    }

    private boolean checkRoomMax(Passage passage) {
        System.out.println("Check Max Num");
        return false;
    }

    private String sqlColumnsToString() {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < sql_columns.length; i++) {
            result.append(sql_columns[i]);
            if (i < sql_columns.length - 1) {
                result.append(", ");
            }
        }
        return result.toString();
    }

    private void writePassageToSQL(Passage passage) {
        String command = "Insert into " + sql_table_to + "(" + sqlColumnsToString() + ") values (?, ?, ?);";
        try {
            PreparedStatement statement = connTo.prepareStatement(command);
            statement.setObject(1, passage.getHour());
            statement.setInt(2, passage.getOriginRoom());
            statement.setInt(3, passage.getDestinationRoom());
            System.out.println(statement.toString());
            int result = statement.executeUpdate();
            statement.close();
        } catch (Exception e) {
            System.out.println("Error Inserting in the database . " + e);
            System.out.println(command);
        }
    }

    private void writeAlertToSQL(String hour, String sensor, String reading, String type, String message) {
        Alert alert = new Alert(hour, null, null, null, type, message);
        System.out.println("Writing " + alert.getMessage() + " to SQL");
    }

}
