import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoTempsToJava {

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

    public ObjectId lastObjectId = null;
    private static long frequency;
    private Temperature last_temp;
    private Double outlier_gap;
    final String[] sql_columns = { "Hora", "Leitura", "Sensor" };
    final DateTimeFormatter date_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private int counter_faulty_data = 0;
    private int counter_outliers = 0;

    public static void main(String[] args) {
        MongoTempsToJava conn = new MongoTempsToJava();

        conn.loadProperties();
        conn.requestWithTimer();
    }

    public void loadProperties() {
        try {
            Properties p = new Properties();
            p.load(new FileInputStream("src/MongoTempsToJava.ini"));
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
            outlier_gap = Double.parseDouble(p.getProperty("outlier_gap"));
        } catch (Exception e) {
            System.out.println("Error reading MongoTempsToJava.ini file " + e);
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
                }
            }
        }, 0, frequency);
    }

    public void deleteOldMongoDocs() {
        mongocol.deleteMany(new Document("_id", new Document("$lte", lastObjectId)));
    }

    public void processData(FindIterable<Document> docs) {
        boolean wroteTempToSQL = false;
        for (Document doc : docs) {
            String hour_string = (String) doc.get("Hora");
            String reading_string = String.valueOf(doc.get("Leitura"));
            String sensor_string = String.valueOf(doc.get("Sensor"));
            if (!containsFaultyData(hour_string, reading_string, sensor_string)) {
                LocalDateTime hour = LocalDateTime.parse(hour_string, date_formatter);
                double reading = Double.valueOf(reading_string);
                int sensor = Integer.valueOf(sensor_string);

                Temperature temperature = new Temperature(hour, reading, sensor);

                if (checkOutliers(temperature)) {
                    writeAlertToSQL(hour_string, sensor_string, reading_string, "Avaria", "Avaria - OUTLIERS");
                }

                wroteTempToSQL = writeTemperatureToSQL(temperature);

            } else {
                writeAlertToSQL(hour_string, sensor_string, reading_string, "Avaria", "Avaria - WRONG DATA");
            }

            if (wroteTempToSQL)
                lastObjectId = (ObjectId) doc.get("_id");
        }

        if (wroteTempToSQL)
            deleteOldMongoDocs();
    }

    private boolean containsFaultyData(String hour, String reading, String sensor) {
        try {
            LocalDateTime.parse(hour, date_formatter);
            Integer.parseInt(reading);
            Integer.parseInt(sensor);
        } catch (Exception e) {
            return true;
        }

        return false;
    }

    private boolean checkOutliers(Temperature current_temp) {
        if (last_temp == null) {
            last_temp = current_temp;
            return false;
        }

        if (declive(last_temp, current_temp) >= outlier_gap || declive(last_temp, current_temp) <= -outlier_gap) {
            last_temp = current_temp;
            counter_outliers++;
            return true;
        }

        return false;
    }

    private double declive(Temperature last_temp, Temperature current_temp) {
        double difference = current_temp.getReading() - last_temp.getReading();
        long duration = Duration.between(current_temp.getHour(), last_temp.getHour()).toSeconds();
        return Math.abs(difference / duration);
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

    private boolean writeTemperatureToSQL(Temperature temperature) {
        String command = "Insert into " + sql_table_to + "(" + SQLColumnsToString() + ") values (?, ?, ?);";
        try {
            PreparedStatement statement = conn_sql.prepareStatement(command);
            statement.setObject(1, temperature.getHour());
            statement.setDouble(2, temperature.getReading());
            statement.setInt(3, temperature.getSensor());
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

    private void writeAlertToSQL(String hour, String sensor, String reading, String type, String message) {
        Alert alert = new Alert(hour, null, sensor, reading, type, message);
        System.out.println("Writing " + alert.getMessage() + " to SQL");
    }

}
