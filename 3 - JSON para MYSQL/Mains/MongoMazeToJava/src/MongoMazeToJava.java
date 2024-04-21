import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.io.*;

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
    static long frequency;
	
    public static void main(String[] args) {
        MongoMazeToJava conn = new MongoMazeToJava();
        
        conn.loadProperties();
        conn.connectMongo();
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
            frequency = Long.parseLong(p.getProperty("frequency"));
        } catch (Exception e) {
            System.out.println("Error reading MongoMazeToJava.ini file " + e);
        }
    }

    public void connectMongo() {
		String mongoURI = new String();
		mongoURI = "mongodb://";		
		if (mongo_authentication.equals("true")) mongoURI = mongoURI + mongo_user + ":" + mongo_password + "@";		
		mongoURI = mongoURI + mongo_address;		
		if (!mongo_replica.equals("false")) 
			if (mongo_authentication.equals("true")) mongoURI = mongoURI + "/?replicaSet=" + mongo_replica+"&authSource=admin";
			else mongoURI = mongoURI + "/?replicaSet=" + mongo_replica;		
		else
			if (mongo_authentication.equals("true")) mongoURI = mongoURI  + "/?authSource=admin";			
		
        MongoClient mongoClient = new MongoClient(new MongoClientURI(mongoURI));
        db = mongoClient.getDatabase(mongo_database);
        mongocol = db.getCollection(mongo_collection);
    }

    public ObjectId lastObjectId = null;
    public void requestWithTimer() {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                FindIterable<Document> docs;
                
                if(lastObjectId == null) {
                    docs = mongocol.find();
                } else {
                    docs = mongocol.find(new Document("_id", new Document("$gt", lastObjectId)));
                    mongocol.deleteMany(new Document("_id", new Document("$lte", lastObjectId)));
                }

                processData(docs);

                System.out.println(lastObjectId.toString() + " - " + mongocol.count() + "\n" );
            }
        }, 0, frequency); 
    }


    public void processData(FindIterable<Document> docs) {
        List<Passage> passages = new ArrayList<>();
        for(Document doc : docs) {
            lastObjectId = (ObjectId) doc.get("_id");
            String hour = (String) doc.get("Hora");
            String originRoom = String.valueOf( (Integer) doc.get("SalaOrigem"));
            String destinationRoom = String.valueOf( (Integer) doc.get("SalaDestino"));
            passages.add(new Passage(hour, originRoom, destinationRoom));
            System.out.println(lastObjectId + " - " + hour + " - " + originRoom + " - " + destinationRoom + "\n");
        }

        checkRoomMax(passages);
        checkInactivity(passages);
    }

    private void checkInactivity(List<Passage> passages) {
        
    }

    private void checkRoomMax(List<Passage> passages) {
        
    }


}
