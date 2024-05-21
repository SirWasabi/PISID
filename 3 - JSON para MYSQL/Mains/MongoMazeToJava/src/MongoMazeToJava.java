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
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;

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
    int default_intermediate_interval;
    LocalDateTime last_intermediate_alert = null;
    final String[] SQL_COLUMNS_PASSAGE = { "Hora", "SalaOrigem", "SalaDestino" };
    final String[] SQL_COLUMNS_ALERT = { "Sala", "SalaOrigem", "SalaDestino", "TipoAlerta", "Mensagem", "HoraEscrita" };
    final DateTimeFormatter date_formatter = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.MICRO_OF_SECOND, 1, 6, true)
            .optionalEnd()
            .toFormatter();

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
            default_intermediate_interval = Integer.parseInt(p.getProperty("default_intermediate_interval"));
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
            e.printStackTrace();
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
                int intermediate_interval = result.getInt("IntervaloIntermedio");
                String date = result.getString("DataComeco");
                LocalDateTime startTime = LocalDateTime.parse(date, date_formatter);

                Maze maze;
                if (experiment == null) {
                    maze = createMaze(id, room_max, number_of_rats);
                    return setupExperiment(id, maze, room_max, number_of_rats, maxWaitTime, intermediate_interval,
                            startTime);
                } else {
                    if (experiment.getID() == id) {
                        System.out.println("THIS EXPERIMENT ALREADY EXISTS. ID: " + experiment.getID());
                        return experiment;
                    } else {
                        maze = createMaze(id, room_max, number_of_rats);
                        return setupExperiment(id, maze, room_max, number_of_rats, maxWaitTime, intermediate_interval,
                                startTime);
                    }
                }
            }

            if (experiment != null && !result.next()) {
                experiment = null;
            }

            result.close();
            statement.close();
        } catch (Exception e) {
            System.out.println("Error Selecting from the database . " + e);
            System.out.println(command);
        }

        return null;
    }

    private Maze createMaze(int id, int room_max, int number_of_rats) {
        File maze_file = new File("src/maze" + id + ".txt");
        if (maze_file.exists()) {
            Maze maze = new Maze(sql_maze_database_connection_to, sql_maze_database_user_to,
                    sql_maze_database_password_to, sql_maze_table_config, sql_maze_table_layout, room_max,
                    number_of_rats);

            try (BufferedReader reader = new BufferedReader(new FileReader("src/maze" + id + ".txt"))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(" - ");
                    if (parts.length == 2) {
                        int roomNumber = Integer.parseInt(parts[0]);
                        int ratsCount = Integer.parseInt(parts[1]);
                        maze.getRoom(roomNumber).setPopulation(ratsCount);
                    }
                }
            } catch (Exception e) {
                System.out.println("ERROR READING MAZE FROM FILE");
            }

            return maze;
        }

        return new Maze(sql_maze_database_connection_to, sql_maze_database_user_to,
                sql_maze_database_password_to, sql_maze_table_config, sql_maze_table_layout, room_max,
                number_of_rats);
    }

    public void saveMazeToFile(Maze maze, int id) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("src/maze" + id + ".txt"))) {
            for (Room room : maze.getRooms()) {
                writer.write(room.getRoomID() + " - " + room.getPopulation());
                writer.newLine();
            }
        } catch (Exception e) {
            System.out.println("ERROR WRITING MAZE TO FILE");
        }
    }

    public int getAdicionalParameters(int id) {
        String command = "SELECT * FROM parametrosadicionais WHERE IdExperiencia = " + id + ";";
        try {
            PreparedStatement statement = conn_sql.prepareStatement(command);
            ResultSet result = statement.executeQuery();

            while (result.next()) {
                int gapRatos = result.getInt("GapRatos");
                return gapRatos;
            }

            result.close();
            statement.close();
        } catch (Exception e) {
            System.out.println("Error Selecting from the database . " + e);
            System.out.println(command);
            return -1;
        }

        System.out.println("ERROR: GAPRATOS RETURNED -1");
        return -1;
    }

    public Experiment setupExperiment(int id, Maze maze, int room_max, int number_of_rats, int maxWaitTime,
            int intermediate_interval, LocalDateTime startTime) {
        experiment = new Experiment(id, maze, maxWaitTime, getAdicionalParameters(id), intermediate_interval,
                startTime);
        System.out.println("EXPERIENCIA ATIVA: " + experiment.getID() + " - NUMERO MAX POR SALA: "
                + maze.getRoom_max() + " - NUMERO DE RATOS: " + maze.getNumber_of_rats() + " - TEMPO MAXIMO DE ESPERA: "
                + experiment.getMaxWaitTime() + " - INTERVALO INTERMEDIO: " + experiment.getIntermediate_interval()
                + " - DATA COMEÇO: " + experiment.getStartTime());
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
        DeleteResult result = mongocol.deleteMany(new Document("_id", new Document("$lte", lastObjectId)));
        System.out.println("ERASED " + result.getDeletedCount() + " DOCS");
    }

    public void deleteAllPreviousMongoDocs() {
        DeleteResult result = mongocol.deleteMany(new Document());
        System.out.println("ERASED " + result.getDeletedCount() + " DOCS BECAUSE NO EXPERIMENT IS ACTIVE");
    }

    public void processData(FindIterable<Document> docs) {
        if (experiment != null) {
            for (Document doc : docs) {
                String hour_string = (String) doc.get("Hora");
                String originRoom_string = String.valueOf((Integer) doc.get("SalaOrigem"));
                String destinationRoom_string = String.valueOf((Integer) doc.get("SalaDestino"));

                Alert wrong_data_alert = containsFaultyData(hour_string, originRoom_string, destinationRoom_string);
                if (wrong_data_alert == null) {
                    LocalDateTime hour = LocalDateTime.parse(hour_string, date_formatter);
                    int originRoom = Integer.valueOf(originRoom_string);
                    int destinationRoom = Integer.valueOf(destinationRoom_string);

                    System.out.println(hour + " - " + experiment.getStartTime() + " - " + hour.isAfter(experiment.getStartTime()));

                    if (hour.isAfter(experiment.getStartTime())) {

                        Maze maze = experiment.getMaze();
                        Passage passage = new Passage(hour, originRoom, destinationRoom);

                        Room origin = maze.getRoom(originRoom);
                        Room destination = maze.getRoom(destinationRoom);
                        if (maze.getRoom(originRoom).getPopulation() != 0) {
                            origin.setPopulation(origin.getPopulation() - 1);
                            destination.setPopulation(destination.getPopulation() + 1);
                        }

                        if (maze.getRoom(destinationRoom)
                                .getPopulation() >= (maze.getRoom_max() - experiment.getGapRatos())
                                && maze.getRoom(destinationRoom).getPopulation() < experiment.getMaze().getRoom_max()) {
                            Alert alert = new Alert(destinationRoom_string, originRoom_string, destinationRoom_string,
                                    null,
                                    null, "INTERMEDIO",
                                    "SALA (" + destinationRoom_string + ") PERTO DE CHEGAR AO SEU MAXIMO", hour_string);
                            if (canSendIntermediateAlert(LocalDateTime.now())) {
                                if (writeAlertToSQL(alert)) {
                                    lastObjectId = (ObjectId) doc.get("_id");
                                    last_intermediate_alert = LocalDateTime.now();
                                }
                            }
                        }

                        for (Room r : maze.getRooms()) {
                            if (r.getRoomID() == originRoom)
                                System.out.println(r.getRoomID() + " - " + r.getPopulation() + " <-- FROM");
                            else if (r.getRoomID() == destinationRoom)
                                System.out.println(r.getRoomID() + " - " + r.getPopulation() + " <-- TO");
                            else
                                System.out.println(r.getRoomID() + " - " + r.getPopulation());
                        }

                        boolean concluded = false;
                        if (maze.checkRoomMax(destinationRoom)) {
                            Alert alert = new Alert(destinationRoom_string, originRoom_string, destinationRoom_string,
                                    null,
                                    null, "CRITICO",
                                    "SALA (" + destinationRoom_string + ") CHEIA", hour_string);
                            if (writeAlertToSQL(alert) && endExperiment()) {
                                lastObjectId = (ObjectId) doc.get("_id");
                                concluded = true;
                                lastPassageDate = null;
                                last_intermediate_alert = null;
                            }
                        }

                        if (lastPassageDate != null) {
                            Duration duration = Duration.between(lastPassageDate, hour);
                            System.out.println("LAST DATE: " + lastPassageDate + " - CURRENT DATE: " + hour);
                            System.out.println("DURATION: " + duration.toSeconds());
                            if (duration.toSeconds() >= experiment.getMaxWaitTime()) {
                                Alert alert = new Alert(destinationRoom_string, originRoom_string,
                                        destinationRoom_string,
                                        null,
                                        null, "CONCLUSAO",
                                        "CONCLUSAO - INATIVIDADE", hour_string);
                                if (writeAlertToSQL(alert) && endExperiment()) {
                                    lastObjectId = (ObjectId) doc.get("_id");
                                    concluded = true;
                                    lastPassageDate = null;
                                    last_intermediate_alert = null;
                                }
                            }
                        }

                        if (writePassageToSQL(passage) && !concluded) {
                            lastObjectId = (ObjectId) doc.get("_id");
                            lastPassageDate = hour;
                            saveMazeToFile(maze, experiment.getID());
                        }
                    }

                } else {
                    if (canSendIntermediateAlert(LocalDateTime.now())) {
                        if (writeAlertToSQL(wrong_data_alert)) {
                            lastObjectId = (ObjectId) doc.get("_id");
                            last_intermediate_alert = LocalDateTime.now();
                        }
                    }
                }
            }
        }

        deleteOldMongoDocs();
    }

    private boolean endExperiment() {
        String command = "UPDATE experiencia SET Estado = 'Concluido' WHERE IdExperiencia = " + experiment.getID()
                + ";";
        try {
            PreparedStatement statement = conn_sql.prepareStatement(command);
            ResultSet result = statement.executeQuery();
            deleteMazeFile(experiment.getID());
            experiment = null;
            result.close();
            statement.close();
            return true;
        } catch (Exception e) {
            System.out.println("Error UPDATING the database . " + e);
            System.out.println(command);
            return false;
        }
    }

    public void deleteMazeFile(int id) {
        File file = new File("src/maze" + id + ".txt");
        if (file.exists())
            file.delete();
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
                    "AVARIA - NON EXISTENT CROSSING: Corredor (" + originRoom + ", " + destinationRoom + ")", hour);
        }

        if (maze.getRoom(Integer.parseInt(originRoom)) == null
                && maze.getRoom(Integer.parseInt(destinationRoom)) == null) {
            return new Alert(null, originRoom, destinationRoom, null, null, "AVARIA",
                    "AVARIA - NON EXISTENT ROOMS: " + originRoom + " AND " + destinationRoom, hour);
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

    private boolean canSendIntermediateAlert(LocalDateTime current_time) {
        if (last_intermediate_alert != null) {
            if (Duration.between(last_intermediate_alert, current_time).toSeconds() > experiment
                    .getIntermediate_interval()) {
                System.out.println("DURATION SINCE LAST ALERT: "
                        + Duration.between(last_intermediate_alert, current_time).toSeconds());
                System.out.println("SENDING ALERT");
                return true;
            }
        } else {
            System.out.println("SENDING ALERT BECAUSE NULL");
            return true;
        }

        System.out.println(
                "DURATION SINCE LAST ALERT: " + Duration.between(last_intermediate_alert, current_time).toSeconds());
        System.out.println("NOT SENDING ALERT");
        return false;
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
