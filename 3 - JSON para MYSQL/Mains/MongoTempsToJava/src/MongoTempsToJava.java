import java.util.ArrayList;
import java.util.Locale;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;

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
    static String sql_table_alert = new String();

    static Connection conn_maze;
    static String sql_maze_database_connection_to = new String();
    static String sql_maze_database_password_to = new String();
    static String sql_maze_database_user_to = new String();
    static String sql_maze_table_config = new String();

    public ObjectId lastObjectId = null;
    static long frequency;
    Double default_temperature_gap;
    Double default_max_temperature_variation;
    Double default_ideal_temperature;
    Double default_outlier_gap;
    int default_intermediate_interval;
    int default_critical_interval;
    Double outlier_gap;
    LocalDateTime last_intermediate_alert = null;
    LocalDateTime last_critical_alert = null;
    final String[] SQL_COLUMNS_TEMPERATURE = { "Hora", "Leitura", "Sensor" };
    final String[] SQL_COLUMNS_ALERT = { "Sensor", "Leitura", "TipoAlerta", "Mensagem", "HoraEscrita" };
    final DateTimeFormatter date_formatter = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .optionalStart()
            .appendPattern(".SSSSSS")
            .optionalEnd()
            .toFormatter();
    Experiment experiment;

    public static void main(String[] args) {
        MongoTempsToJava conn = new MongoTempsToJava();

        conn.loadProperties();
        conn.connectMazeMySQL();
        conn.getMazeConfig();
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
            sql_table_alert = p.getProperty("sql_table_alert");
            sql_database_connection_to = p.getProperty("sql_database_connection_to");
            sql_database_password_to = p.getProperty("sql_database_password_to");
            sql_database_user_to = p.getProperty("sql_database_user_to");

            sql_maze_table_config = p.getProperty("sql_maze_table_config");
            sql_maze_database_connection_to = p.getProperty("sql_maze_database_connection_to");
            sql_maze_database_password_to = p.getProperty("sql_maze_database_password_to");
            sql_maze_database_user_to = p.getProperty("sql_maze_database_user_to");

            frequency = Long.parseLong(p.getProperty("frequency"));
            default_temperature_gap = Double.parseDouble(p.getProperty("default_temperature_gap"));
            default_max_temperature_variation = Double.parseDouble(p.getProperty("default_max_temperature_variation"));
            default_outlier_gap = Double.parseDouble(p.getProperty("default_outlier_gap"));
            default_intermediate_interval = Integer.parseInt(p.getProperty("default_intermediate_interval"));
            default_critical_interval = Integer.parseInt(p.getProperty("default_critical_interval"));
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
        System.out.println("GETTING MAZE INFO");
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
                default_ideal_temperature = Double.parseDouble(result.getString("temperaturaprogramada"));
            }

            result.close();
            statement.close();
        } catch (Exception e) {
            System.out.println("Error Selecting from the database . " + e);
            System.out.println(command);
        }
    }

    private Experiment createExperimentIfActive() {
        String command = "SELECT * FROM experiencia WHERE Estado='Ativo';";
        try {
            PreparedStatement statement = conn_sql.prepareStatement(command);
            ResultSet result = statement.executeQuery();

            while (result.next()) {
                int id = result.getInt("IDExperiencia");
                double ideal_temperature = default_ideal_temperature;
                double max_temperature_variation = default_max_temperature_variation;
                int intermediate_interval = default_intermediate_interval;

                if (result.getObject("TemperaturaIdeal") != null) {
                    ideal_temperature = result.getDouble("TemperaturaIdeal");
                }
                if (result.getObject("VariacaoTemperaturaMaxima") != null) {
                    max_temperature_variation = result.getDouble("VariacaoTemperaturaMaxima");
                }
                if (result.getObject("IntervaloIntermedio") != null) {
                    intermediate_interval = result.getInt("IntervaloIntermedio");
                }

                return setupExperiment(id, ideal_temperature, max_temperature_variation, intermediate_interval);
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

    public ArrayList<Double> getAdicionalParameters(int id) {
        String command = "SELECT * FROM parametrosadicionais WHERE IdExperiencia = " + id + ";";
        try {
            PreparedStatement statement = conn_sql.prepareStatement(command);
            ResultSet result = statement.executeQuery();
            ArrayList<Double> parameters = new ArrayList<>();

            while (result.next()) {
                parameters.add(result.getDouble("GapTemperatura"));
                if (result.getObject("GapOutliers") != null) {
                    parameters.add(result.getDouble("GapOutliers"));
                } else {
                    parameters.add(default_outlier_gap);
                }

                return parameters;
            }

            result.close();
            statement.close();
        } catch (Exception e) {
            System.out.println("Error Selecting from the database . " + e);
            System.out.println(command);
            return null;
        }

        System.out.println("ERROR: FAILED TO GET ADITIONAL PARAMETERS");
        return null;
    }

    public Experiment setupExperiment(int id, double ideal_temperature, double max_temperature_variation,
            int intermediate_interval) {
        ArrayList<Double> parameters = getAdicionalParameters(id);
        if (experiment == null) {
            experiment = new Experiment(id, ideal_temperature, max_temperature_variation, parameters.get(0),
                    parameters.get(1), intermediate_interval);
            System.out.println(intermediate_interval);
            System.out.println("EXPERIENCIA ATIVA - ID: " + experiment.getID() + " - TEMPERATURA IDEAL: "
                    + experiment.getIdealTemperature() + " - VARIACAO MAXIMA DE TEMPERATURA: "
                    + experiment.getMaxTemperatureVariation()
                    + " - GAP DE TEMPERATURA: " + experiment.getTemperatureGap()
                    + " - GAP DE OUTLIERS: " + experiment.getOutlierGap()
                    + " - INTERVALO INTERMEDIO: " + experiment.getIntermediate_interval());
            return experiment;
        } else {
            if (experiment.getID() == id) {
                System.out.println(
                        "THIS EXPERIMENT ALREADY EXISTS - ID: " + experiment.getID() + " - TEMPERATURA IDEAL: "
                                + experiment.getIdealTemperature() + " - VARIACAO MAXIMA DE TEMPERATURA: "
                                + experiment.getMaxTemperatureVariation()
                                + " - GAP DE TEMPERATURA: " + experiment.getTemperatureGap()
                                + " - GAP DE OUTLIERS: " + experiment.getOutlierGap()
                                + " - INTERVALO INTERMEDIO: " + experiment.getIntermediate_interval());
                return experiment;
            } else {
                experiment = new Experiment(id, ideal_temperature, max_temperature_variation, parameters.get(0),
                        parameters.get(1), intermediate_interval);
                System.out.println("EXPERIENCIA ATIVA - ID" + experiment.getID() + " - TEMPERATURA IDEAL: "
                        + experiment.getIdealTemperature() + " - VARIACAO MAXIMA DE TEMPERATURA: "
                        + experiment.getMaxTemperatureVariation()
                        + " - GAP DE TEMPERATURA: " + experiment.getTemperatureGap()
                        + " - GAP DE OUTLIERS: " + experiment.getOutlierGap()
                        + " - INTERVALO INTERMEDIO: " + experiment.getIntermediate_interval());
                return experiment;
            }
        }
    }

    private boolean endExperiment() {
        String command = "UPDATE experiencia SET Estado = 'Concluido' WHERE IdExperiencia = " + experiment.getID()
                + ";";
        try {
            PreparedStatement statement = conn_sql.prepareStatement(command);
            ResultSet result = statement.executeQuery();
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

    public void processData(FindIterable<Document> docs) {
        for (Document doc : docs) {
            String hour_string = (String) doc.get("Hora");
            String reading_string = String.valueOf(doc.get("Leitura"));
            String sensor_string = String.valueOf(doc.get("Sensor"));

            Alert wrong_data_alert = containsFaultyData(hour_string, reading_string, sensor_string);
            if (wrong_data_alert == null) {
                LocalDateTime hour = LocalDateTime.parse(hour_string, date_formatter);
                DecimalFormat df = new DecimalFormat("#.##", new DecimalFormatSymbols(Locale.US));
                double reading = Double.parseDouble(df.format(Double.valueOf(reading_string)));
                int sensor_id = Integer.valueOf(sensor_string);

                Temperature temperature = new Temperature(hour, reading, sensor_id);
                Sensor sensor;
                boolean concluded = false;

                if (experiment != null) {
                    if (!experiment.existsSensor(sensor_id)) {
                        sensor = new Sensor(sensor_id);
                        experiment.getSensors().add(sensor);
                    } else {
                        sensor = experiment.getSensor(sensor_id);
                    }

                    if (checkOutliers(sensor, temperature)) {
                        Alert alert = new Alert(null, null, null, sensor_string, reading_string, "AVARIA",
                                "AVARIA - OUTLIERS - SENSOR: " + temperature.getSensor() + " - TEMPERATURA ANTIGA: "
                                        + sensor.getLastTemperature().getReading() + " - TEMPERATURA NOVA: "
                                        + temperature.getReading() + " - GAP DE OUTLIERS: "
                                        + experiment.getOutlierGap(),
                                hour_string);
                        if (canSendIntermediateAlert(LocalDateTime.now())) {
                            if (writeAlertToSQL(alert)) {
                                lastObjectId = (ObjectId) doc.get("_id");
                                last_intermediate_alert = LocalDateTime.now();
                            }
                        }
                    } else {
                        if (checkIntermediateVariation(temperature)) {
                            Alert alert = new Alert(null, null, null, sensor_string, reading_string, "INTERMEDIO",
                                    "TEMPERATURA PERTO DA VARIACAO MAXIMA - SENSOR: " + temperature.getSensor()
                                            + " - TEMPERATURA: "
                                            + temperature.getReading(),
                                    hour_string);
                            if (canSendIntermediateAlert(LocalDateTime.now())) {
                                if (writeAlertToSQL(alert)) {
                                    lastObjectId = (ObjectId) doc.get("_id");
                                    last_intermediate_alert = LocalDateTime.now();
                                }
                            }
                        }
                        if (checkCriticalVariation(temperature)) {
                            Alert alert = new Alert(null, null, null, sensor_string, reading_string, "CRITICO",
                                    "TEMPERATURA FORA DA VARIACAO MAXIMA - SENSOR: " + temperature.getSensor()
                                            + " - TEMPERATURA: "
                                            + temperature.getReading(),
                                    hour_string);
                            if (canSendIntermediateAlert(LocalDateTime.now())) {
                                if (writeAlertToSQL(alert) && endExperiment()) {
                                    lastObjectId = (ObjectId) doc.get("_id");
                                    concluded = true;
                                }
                            }
                        }
                    }
                } else {
                    System.out.println("DEFAULT VALUES");
                    if (checkIntermediateVariation(temperature)) {
                        Alert alert = new Alert(null, null, null, sensor_string, reading_string, "INTERMEDIO",
                                "TEMPERATURA FORA DA EXPERIENCIA PERTO DA VARIACAO MAXIMA DEFAULT - SENSOR: "
                                        + temperature.getSensor()
                                        + " - TEMPERATURA: "
                                        + temperature.getReading(),
                                hour_string);
                        if (canSendIntermediateAlert(LocalDateTime.now())) {
                            if (writeAlertToSQL(alert)) {
                                lastObjectId = (ObjectId) doc.get("_id");
                                last_intermediate_alert = LocalDateTime.now();
                            }
                        }
                    }

                    if (checkCriticalVariation(temperature)) {
                        Alert alert = new Alert(null, null, null, sensor_string, reading_string, "CRITICO",
                                "TEMPERATURA FORA DA EXPERIENCIA FORA DA VARIACAO MAXIMA DEFAULT - SENSOR: "
                                        + temperature.getSensor()
                                        + " - TEMPERATURA: "
                                        + temperature.getReading(),
                                hour_string);
                        if (canSendCriticalAlert(LocalDateTime.now())) {
                            if (writeAlertToSQL(alert)) {
                                lastObjectId = (ObjectId) doc.get("_id");
                                last_critical_alert = LocalDateTime.now();
                            }
                        }
                    }
                }

                if (writeTemperatureToSQL(temperature) && !concluded) {
                    lastObjectId = (ObjectId) doc.get("_id");
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
        deleteOldMongoDocs();
    }

    private boolean checkIntermediateVariation(Temperature temperature) {
        double maxGapValue, minGapValue;
        double max_temperature, min_temperature;
        if (experiment != null) {
            double idealTemp = experiment.getIdealTemperature();
            maxGapValue = idealTemp + experiment.getTemperatureGap();
            minGapValue = idealTemp - experiment.getTemperatureGap();
            max_temperature = idealTemp + experiment.getMaxTemperatureVariation();
            min_temperature = idealTemp - experiment.getMaxTemperatureVariation();
            if ((temperature.getReading() >= maxGapValue
                    && temperature.getReading() < max_temperature)) {
                System.out.println("TEMPERATURA LIDA: " + temperature.getReading() + " - TEMPERATURA IDEAL: "
                        + idealTemp + " - MAX GAP: " + maxGapValue + " - MAX TEMPERATURA: " + max_temperature);
                return true;
            }
            if (temperature.getReading() <= minGapValue
                    && temperature.getReading() > min_temperature) {
                System.out.println("TEMPERATURA LIDA: " + temperature.getReading() + " - TEMPERATURA IDEAL: "
                        + idealTemp + " - MIN GAP: " + minGapValue + " - MIN TEMPERATURA: " + min_temperature);
                return true;
            }
        } else {
            maxGapValue = default_ideal_temperature + default_temperature_gap;
            minGapValue = default_ideal_temperature - default_temperature_gap;
            max_temperature = default_ideal_temperature + default_max_temperature_variation;
            min_temperature = default_ideal_temperature - default_max_temperature_variation;
            if (temperature.getReading() >= maxGapValue && temperature.getReading() < max_temperature) {
                System.out.println("TEMPERATURA LIDA: " + temperature.getReading() + " - TEMPERATURA IDEAL: "
                        + default_ideal_temperature + " - MAX GAP: " + maxGapValue + " - MAX TEMPERATURA: "
                        + max_temperature);
                return true;
            }
            if (temperature.getReading() <= minGapValue && temperature.getReading() > min_temperature) {
                System.out.println("TEMPERATURA LIDA: " + temperature.getReading() + " - TEMPERATURA IDEAL: "
                        + default_ideal_temperature + " - MIN GAP: " + minGapValue + " - MIN TEMPERATURA: "
                        + min_temperature);
                return true;
            }
        }

        return false;
    }

    private boolean checkCriticalVariation(Temperature temperature) {
        double max_temperature, min_temperature;
        if (experiment != null) {
            double idealTemp = experiment.getIdealTemperature();
            max_temperature = idealTemp + experiment.getMaxTemperatureVariation();
            min_temperature = idealTemp - experiment.getMaxTemperatureVariation();
            if (temperature.getReading() >= max_temperature
                    || temperature.getReading() <= min_temperature) {
                System.out
                        .println("TEMPERATURA LIDA: " + temperature.getReading() + " - TEMPERATURA IDEAL: " + idealTemp
                                + " - MIN TEMPERATURA: " + min_temperature + " - MAX TEMPERATURA: " + max_temperature);
                return true;
            }
        } else {
            max_temperature = default_ideal_temperature + default_max_temperature_variation;
            min_temperature = default_ideal_temperature - default_max_temperature_variation;
            if (temperature.getReading() >= max_temperature
                    || temperature.getReading() <= min_temperature) {
                System.out.println("TEMPERATURA LIDA: " + temperature.getReading() + " - TEMPERATURA IDEAL: "
                        + default_ideal_temperature + " - MIN TEMPERATURA: " + min_temperature + " - MAX TEMPERATURA: "
                        + max_temperature);
                return true;
            }
        }
        return false;
    }

    private Alert containsFaultyData(String hour, String reading, String sensor) {
        try {
            LocalDateTime.parse(hour, date_formatter);
            Double.parseDouble(reading);
            Integer.parseInt(sensor);
        } catch (Exception e) {
            return new Alert(null, null, null, sensor, reading, "AVARIA",
                    "AVARIA - WRONG DATA FORMAT - SENSOR: " + sensor + " - LEITURA: " + reading
                            + " - HORA: " + hour,
                    hour);
        }

        return null;
    }

    private boolean checkOutliers(Sensor sensor, Temperature current_temp) {
        if (sensor.getLastTemperature() == null) {
            sensor.setLastTemperature(current_temp);
            return false;
        }

        if (declive(sensor.getLastTemperature(), current_temp) >= experiment.getOutlierGap()
                || declive(sensor.getLastTemperature(), current_temp) <= -experiment.getOutlierGap()) {
            sensor.setLastTemperature(current_temp);
            return true;
        }

        return false;
    }

    private double declive(Temperature last_temp, Temperature current_temp) {
        double difference = current_temp.getReading() - last_temp.getReading();
        long duration = Duration.between(current_temp.getHour(), last_temp.getHour()).toSeconds();
        return Math.abs(difference / duration);
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

    private boolean writeTemperatureToSQL(Temperature temperature) {
        String command = "Insert into " + sql_table_to + "(" + SQLColumnsToString(SQL_COLUMNS_TEMPERATURE)
                + ") values (?, ?, ?);";
        try {
            PreparedStatement statement = conn_sql.prepareStatement(command);
            statement.setObject(1, temperature.getHour());
            statement.setDouble(2, temperature.getReading());
            statement.setInt(3, temperature.getSensor());
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
                        System.out.println("DURATION SINCE LAST ALERT: " + Duration.between(last_intermediate_alert, current_time).toSeconds());
                        System.out.println("SENDING ALERT");
                return true;
            }
        } else {
            System.out.println("SENDING ALERT BECAUSE NULL");
            return true;
        }

        System.out.println("DURATION SINCE LAST ALERT: " + Duration.between(last_intermediate_alert, current_time).toSeconds());
        System.out.println("NOT SENDING ALERT");
        return false;
    }

    private boolean canSendCriticalAlert(LocalDateTime current_time) {
        if (last_critical_alert != null) {
            if (Duration.between(last_critical_alert, current_time).toSeconds() > default_critical_interval) {
                System.out.println("DURATION SINCE LAST ALERT: " + Duration.between(last_critical_alert, current_time).toSeconds());
                return true;
            }
        } else {
            System.out.println("SENDING ALERT BECAUSE NULL");
            return true;
        }

        System.out.println("NOT SENDING ALERT");
        return false;
    }

    private boolean writeAlertToSQL(Alert alert) {
        String command = "Insert into " + sql_table_alert + " (" + SQLColumnsToString(SQL_COLUMNS_ALERT)
                + ") values (?, ?, ?, ?, ?);";
        try {
            PreparedStatement statement = conn_sql.prepareStatement(command);
            statement.setString(1, alert.getSensor());
            statement.setString(2, alert.getReading());
            statement.setString(3, alert.getType());
            statement.setString(4, alert.getMessage());
            statement.setString(5, alert.getHour());
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
