/**
 * Author: Vatsala Singh
 */
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;

public class DenormalizedMigration {
    private static String DB_URL = "jdbc:mysql://localhost:3306/imdb";
    private static String DB_user = "root";
    private static String DB_passwd = "7542";
    // method to connect to MySQL
    public static Connection getConnection() throws SQLException {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Connection conn = DriverManager.getConnection(DB_URL, DB_user, DB_passwd);
        System.err.println("Connection established");
        return conn;
    }
    // method to connect to MongoDB
    public static MongoClient getMongoConnection() {
        MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
        return mongoClient;
    }
    public static MongoDatabase createMongoDB(MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase("denormalizedIMDB");
        return database;
    }
    public static void fetchFromMovies(MongoDatabase db) throws SQLException {
        MongoCollection movies = db.getCollection("movies");
        movies.drop();
        movies = db.getCollection("movies");
        Connection newConn1 = getConnection();
        Statement statement = newConn1.createStatement();
        List<Document> docs = new ArrayList<>();
        HashMap<Integer, ArrayList<Document>> moviegenre = new HashMap<>();
        HashMap<Integer, ArrayList<Document>> moviewithDirInfo = new HashMap<>();
        HashMap<Integer, ArrayList<Document>> moviewithActorInfo = new HashMap<>();
        try {
            newConn1.setAutoCommit(false);
            String str = "SELECT * FROM movies";
            statement.setFetchSize(10000);
            ResultSet rs = statement.executeQuery(str);
            while (rs.next()) {
                Document dbObject = new Document();
                int id = rs.getInt("id");
                String title = rs.getString("title");
                String year = rs.getString("year");
                dbObject.put("_id", id);
                dbObject.put("title", title);
                dbObject.put("year", year);
                docs.add(dbObject);

            }
            movies.insertMany(docs);
            System.out.println("Inserted movie details");
            System.out.println("Joining directors and directedby");
            String str2 = "SELECT d.id, d.first, d.last, db.movieid FROM directors AS d JOIN directedby AS db ON d.id = db.directorid";
            statement.setFetchSize(1000);
            ResultSet directorRs = statement.executeQuery(str2);
            while (directorRs.next()) {
                Document document = new Document();
                int movieToAppendFromdir = directorRs.getInt("db.movieid");
                int directorid = directorRs.getInt("d.id");
                String dirFirst = directorRs.getString("d.first");
                String dirLast = directorRs.getString("d.last");
                ArrayList<Document> dirInfo = new ArrayList<>();
                if (moviewithDirInfo.containsKey(movieToAppendFromdir)) {
                    dirInfo = moviewithDirInfo.get(movieToAppendFromdir);
                }
                document.put("directorid", directorid);
                document.put("first", dirFirst);
                document.put("last", dirLast);
                dirInfo.add(document);
                moviewithDirInfo.put(movieToAppendFromdir, dirInfo);
            }
            for (Map.Entry<Integer, ArrayList<Document>> entry: moviewithDirInfo.entrySet()) {
                movies.updateOne(eq("_id", entry.getKey()), combine(set("directors", entry.getValue())));
            }
            System.out.println("Joining movie and movie genre");
            // inserting genres in the movies collection
            String str1 = "SELECT mg.movieid, g.genre, g.id FROM moviegenres AS mg JOIN genres AS g ON mg.genreid = g.id";
            statement.setFetchSize(1000);
            ResultSet movieAndGenreRs = statement.executeQuery(str1);
            while (movieAndGenreRs.next()) {
                Document document1 = new Document();
                int movieToAppend = movieAndGenreRs.getInt("mg.movieid");
                String genre = movieAndGenreRs.getString("g.genre");
                int genreid = movieAndGenreRs.getInt("g.id");
                ArrayList<Document> genreIdAndName = new ArrayList<>();
                if (moviegenre.containsKey(movieToAppend)) {
                    genreIdAndName = moviegenre.get(movieToAppend);
                }
                document1.put("genreid", genreid);
                document1.put("genre", genre);
                genreIdAndName.add(document1);
                moviegenre.put(movieToAppend, genreIdAndName);
            }
            for (Map.Entry<Integer, ArrayList<Document>> entry: moviegenre.entrySet()) {
                movies.updateOne(eq("_id", entry.getKey()), combine(set("genres", entry.getValue())));
            }
            System.out.println("Joining actors and roles");
            // inserting genres in the movies collection
            String str3 = "SELECT a.id, a.first, a.last, r.movieid, r.role, a.gender FROM actors AS a JOIN roles AS r ON a.id = r.actorid";
            statement.setFetchSize(1000);
            ResultSet actorAndRolesRS = statement.executeQuery(str3);
            while (actorAndRolesRS.next()) {
                Document document = new Document();
                int movieFromRoles = actorAndRolesRS.getInt("r.movieid");
                int actorid = actorAndRolesRS.getInt("a.id");
                String actorFirst = actorAndRolesRS.getString("a.first");
                String actorLast = actorAndRolesRS.getString("a.last");
                String gender = actorAndRolesRS.getString("a.gender");
                String role = actorAndRolesRS.getString("r.role");
                ArrayList<Document> actorInfo = new ArrayList<>();
                if (moviewithActorInfo.containsKey(movieFromRoles)) {
                    actorInfo = moviewithActorInfo.get(movieFromRoles);
                }
                document.put("_id", actorid);
                document.put("first", actorFirst);
                document.put("last", actorLast);
                document.put("gender", gender);
                document.put("role", role);
                actorInfo.add(document);
                moviewithActorInfo.put(movieFromRoles, actorInfo);
            }
            for (Map.Entry<Integer, ArrayList<Document>> entry: moviewithActorInfo.entrySet()) {
                movies.updateOne(eq("_id", entry.getKey()), combine(set("actors", entry.getValue())));
            }
            newConn1.commit();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            System.out.println("rolling back");
            newConn1.rollback();
        }
    }
    public static void main(String[] args) throws SQLException {
        MongoClient mongoClient = getMongoConnection();
        MongoDatabase db = createMongoDB(mongoClient);
        fetchFromMovies(db);
        mongoClient.close();
    }
}
