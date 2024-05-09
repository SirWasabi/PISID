import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

    Experiment experiment;
    ObjectId lastObjectId = null;
    LocalDateTime lastPassageDate = null;
    long frequency;
    final String[] SQL_COLUMNS_PASSAGE = { "Hora", "SalaOrigem", "SalaDestino" };
    final String[] SQL_COLUMNS_ALERT = { "Sala", "SalaOrigem", "SalaDestino", "TipoAlerta", "Mensagem", "HoraEscrita" };
    final DateTimeFormatter date_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    public static void main(String[] args) {
        MongoMazeToJava conn = new MongoMazeToJava();

        conn.loadProperties();
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

    private Experiment createExperimentIfActive() {
        String command = "SELECT * FROM experiencia WHERE Estado='Ativo';";
        try {
            PreparedStatement statement = conn_sql.prepareStatement(command);
            ResultSet result = statement.executeQuery();

            while (result.next()) {
                int id = result.getInt("IDExperiencia");
                int room_max = result.getInt("LimiteRatos");
                int number_of_rats = result.getInt("NumeroRatos");
                int maxWaitTime = result.getInt("SegundoSemMovimento");

                if (experiment == null) {
                    System.out.println(
                            "EXPERIENCIA ATIVA: " + id + " - NUMERO MAX: " + room_max + " - NUMERO DE RATOS: "
                                    + number_of_rats);
                    return setupExperiment(id, room_max, number_of_rats, maxWaitTime);
                } else {
                    if (experiment.getID() == id) {
                        System.out.println("THIS EXPERIMENT ALREADY EXISTS. ID: " + experiment.getID());
                        return experiment;
                    } else {
                        setupExperiment(id, room_max, number_of_rats, maxWaitTime);
                    }
                }
            }

            result.close();
            statement.close();
        } catch (Exception e) {
            System.out.println("Error Selecting from the database . " + e);
            System.out.println(command);
        }

        return null;
    }

    public Experiment setupExperiment(int id, int room_max, int number_of_rats, int maxWaitTime) {
        Maze maze = new Maze(sql_maze_database_connection_to, sql_maze_database_user_to,
                sql_maze_database_password_to, sql_maze_table_config, sql_maze_table_layout, room_max,
                number_of_rats);
        experiment = new Experiment(id, maze, maxWaitTime);
        System.out.println("EXPERIENCIA ATIVA: " + experiment.getID() + " - NUMERO MAX SALA: "
                + maze.getRoom_max() + " - NUMERO DE RATOS: " + maze.getNumber_of_rats());
        return experiment;
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
                        createExperimentIfActive();
                        processData(docs);
                    }
                } else {
                    System.out.println("ERROR CONNECTING");
                }
            }
        }, 0, frequency);
    }

    public void deleteOldMongoDocs() {
        System.out.println("ERASED " + mongocol.count() + " DOCS");
        mongocol.deleteMany(new Document("_id", new Document("$lte", lastObjectId)));
    }

    public void processData(FindIterable<Document> docs) {
        for (Document doc : docs) {
            String hour_string = (String) doc.get("Hora");
            String originRoom_string = String.valueOf((Integer) doc.get("SalaOrigem"));
            String destinationRoom_string = String.valueOf((Integer) doc.get("SalaDestino"));

            if (experiment != null) {
                Alert wrong_data = containsFaultyData(hour_string, originRoom_string, destinationRoom_string);
                if (wrong_data == null) {
                    LocalDateTime hour = LocalDateTime.parse(hour_string, date_formatter);
                    int originRoom = Integer.valueOf(originRoom_string);
                    int destinationRoom = Integer.valueOf(destinationRoom_string);

                    Maze maze = experiment.getMaze();
                    Passage passage = new Passage(hour, originRoom, destinationRoom);

                    Room origin = maze.getRoom(originRoom);
                    Room destination = maze.getRoom(destinationRoom);
                    if (maze.getRoom(originRoom).getPopulation() != 0) {
                        origin.setPopulation(origin.getPopulation() - 1);
                        destination.setPopulation(destination.getPopulation() + 1);
                    }
                    
                    // TODO GAP RATOS

                    for (Room r : maze.getRooms()) {
                        if (r.getRoomID() == originRoom)
                            System.out.println(r.getRoomID() + " - " + r.getPopulation() + " <-- FROM");
                        else if (r.getRoomID() == destinationRoom)
                            System.out.println(r.getRoomID() + " - " + r.getPopulation() + " <-- TO");
                        else
                            System.out.println(r.getRoomID() + " - " + r.getPopulation());
                    }

                    boolean ending_experiment = false;
                    if (maze.checkRoomMax(destinationRoom)) {
                        Alert alert = new Alert(destinationRoom_string, originRoom_string, destinationRoom_string, null,
                                null, "CRITICO",
                                "SALA " + destinationRoom_string + " CHEIA", hour_string);
                        if (writeAlertToSQL(alert)) {
                            lastObjectId = (ObjectId) doc.get("_id");
                            ending_experiment = true;
                        }
                    }

                    if (lastPassageDate != null) {
                        Duration duration = Duration.between(lastPassageDate, hour);
                        if (duration.toSeconds() >= experiment.getMaxWaitTime()) {
                            Alert alert = new Alert(destinationRoom_string, originRoom_string, destinationRoom_string,
                                    null,
                                    null, "CONCLUSAO",
                                    "CONCLUSAO - INATIVIDADE", hour_string);
                            writeAlertToSQL(alert);
                            endExperiment();
                        }
                    }

                    if (writePassageToSQL(passage) && !ending_experiment) {
                        lastObjectId = (ObjectId) doc.get("_id");
                        lastPassageDate = hour;
                    }

                } else {
                    Alert alert = new Alert(null, originRoom_string, destinationRoom_string, null, null, "AVARIA",
                            "Avaria - WRONG DATA", hour_string);
                    if (writeAlertToSQL(alert))
                        lastObjectId = (ObjectId) doc.get("_id");
                }
            }
        }

        deleteOldMongoDocs();
    }

    private void endExperiment() {
        String command = "UPDATE experiencia SET Estado = 'Concluido' WHERE IdExperiencia = " + experiment.getID() + ";";
        try {
            PreparedStatement statement = conn_sql.prepareStatement(command);
            ResultSet result = statement.executeQuery();
            result.close();
            statement.close();
        } catch (Exception e) {
            System.out.println("Error UPDATING the database . " + e);
            System.out.println(command);
        }
    }

    private Alert containsFaultyData(String hour, String originRoom, String destinationRoom) {
        try {
            LocalDateTime.parse(hour, date_formatter);
            Integer.parseInt(originRoom);
            Integer.parseInt(destinationRoom);
        } catch (Exception e) {
            return new Alert(null, originRoom, destinationRoom, null, null, "AVARIA",
                    "AVARIA - WRONG DATA FORMAT - SALA ORIGEM: " + originRoom + " - SALA DESTINO: " + destinationRoom
                            + " - HORA: " + hour,
                    hour);
        }

        Maze maze = experiment.getMaze();
        if (!maze.existsCrossing(Integer.parseInt(originRoom), Integer.parseInt(destinationRoom))) {
            return new Alert(null, originRoom, destinationRoom, null, null, "AVARIA",
                    "AVARIA - NON EXISTENT CROSSING: Corredor(" + originRoom + ", " + destinationRoom + ")", hour);
        }

        if (maze.getRoom(Integer.parseInt(originRoom)) == null
                && maze.getRoom(Integer.parseInt(destinationRoom)) == null) {
            return new Alert(null, originRoom, destinationRoom, null, null, "AVARIA",
                    "AVARIA - NON EXISTENT ROOMS: " + originRoom + " - " + destinationRoom, hour);
        }

        if (maze.getRoom(Integer.parseInt(originRoom)) == null) {
            return new Alert(null, originRoom, destinationRoom, null, null, "AVARIA",
                    "AVARIA - NON EXISTENT ORIGIN ROOM: " + originRoom, hour);
        }

        if (maze.getRoom(Integer.parseInt(destinationRoom)) == null) {
            return new Alert(null, originRoom, destinationRoom, null, null, "AVARIA",
                    "AVARIA - NON EXISTENT DESTINATION ROOM: " + destinationRoom, hour);
        }

        if (maze.getRoom(Integer.parseInt(originRoom)).getPopulation() == 0) {
            return new Alert(null, originRoom, destinationRoom, null, null, "AVARIA",
                    "AVARIA - NO RATS IN ORIGIN ROOM: " + originRoom, hour);
        }

        return null;
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
        String command = "Insert into " + sql_table_to + " (" + SQLColumnsToString(SQL_COLUMNS_PASSAGE)
                + ") values (?, ?, ?);";
        try {
            PreparedStatement statement = conn_sql.prepareStatement(command);
            statement.setObject(1, passage.getHour());
            statement.setInt(2, passage.getOriginRoom());
            statement.setInt(3, passage.getDestinationRoom());
            System.out.println(statement.toString());
            if (!conn_sql.isClosed()) {
                statement.executeUpdate();
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
        String command = "Insert into " + sql_table_alert + " (" + SQLColumnsToString(SQL_COLUMNS_ALERT)
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
                statement.executeUpdate();
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
