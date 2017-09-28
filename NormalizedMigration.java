/**
 * Author: Vatsala Singh
 */

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import org.bson.Document;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class NormalizedMigration {
    private static String DB_URL = "jdbc:mysql://localhost:3306/imdb";
    private static String DB_user = "root";
    private static String DB_passwd = "7542";
    private Statement statement = null;
    private Connection connection = null;
    // method to get a MySQL connection
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
    // method to get a MongoDB connection
    public static MongoClient getMongoConnection() {
        MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
        return mongoClient;
    }
    // create a mongodb database
    public static MongoDatabase createMongoDB(MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase("imdbInMongo");
        return database;
    }
    // populate actors collection
    public static void fetchFromActors(MongoClient mongoClient, MongoDatabase db) throws SQLException {
        MongoCollection actors = db.getCollection("actors");
        Connection newConn1 = getConnection();
        Statement statement = newConn1.createStatement();
        List<Document> docs = new ArrayList<>();
        try {
            newConn1.setAutoCommit(false);
            String str = "SELECT * FROM actors";
            statement.setFetchSize(10000);
            ResultSet rs = statement.executeQuery(str);
            //int count = 0;
            while (rs.next()) {
                Document dbObject = new Document();
                int id = rs.getInt("id");
                String first = rs.getString("first");
                String last = rs.getString("last");
                String gender = rs.getString("gender");
                dbObject.put("_id", id);
                dbObject.put("first", first);
                dbObject.put("last", last);
                dbObject.put("gender", gender);
                docs.add(dbObject);
                actors.insertOne(dbObject);
            }
            System.out.println("fetched everything");
            newConn1.commit();
        } catch (SQLException e) {
            // if database exists
            System.out.println(e.getMessage());
            System.out.println("rolling back");
            newConn1.rollback();
        }
    }
    public static void fetchFromDirectors(MongoClient mongoClient, MongoDatabase db, Connection newConn1) throws SQLException {
        MongoCollection directors = db.getCollection("directors");
        Statement statement = newConn1.createStatement();
        List<Document> docs = new ArrayList<>();
        try {
            newConn1.setAutoCommit(false);
            String str = "SELECT * FROM directors";
            statement.setFetchSize(20000);
            ResultSet rs = statement.executeQuery(str);
            while (rs.next()) {
                Document dbObject = new Document();
                int id = rs.getInt("id");
                String first = rs.getString("first");
                String last = rs.getString("last");
                dbObject.put("_id", id);
                dbObject.put("first", first);
                dbObject.put("last", last);
                docs.add(dbObject);
            }
            System.out.println("fetched everything");
            directors.insertMany(docs);
            newConn1.commit();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            System.out.println("rolling back");
            newConn1.rollback();
        }
    }
    public static void fetchFromMovies(MongoClient mongoClient, MongoDatabase db, Connection newConn1) throws SQLException {
        MongoCollection movies = db.getCollection("movies");
        Statement statement = newConn1.createStatement();
        List<Document> docs = new ArrayList<>();
        try {
            newConn1.setAutoCommit(false);
            String str = "SELECT * FROM movies";
            statement.setFetchSize(20000);
            ResultSet rs = statement.executeQuery(str);
            while (rs.next()) {
                Document dbObject = new Document();
                int id = rs.getInt("id");
                String title = rs.getString("title");
                int year = rs.getInt("year");
                dbObject.put("_id", id);
                dbObject.put("title", title);
                dbObject.put("year", year);
                docs.add(dbObject);
            }
            System.out.println("fetched everything");
            movies.insertMany(docs);
            newConn1.commit();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            System.out.println("rolling back");
            newConn1.rollback();
        }
    }
    public static void fetchFromgenres(MongoClient mongoClient, MongoDatabase db, Connection newConn1) throws SQLException {
        MongoCollection genres = db.getCollection("genres");
        Statement statement = newConn1.createStatement();
        List<Document> docs = new ArrayList<>();
        try {
            newConn1.setAutoCommit(false);
            String str = "SELECT * FROM genres";
            statement.setFetchSize(20000);
            ResultSet rs = statement.executeQuery(str);
            while (rs.next()) {
                Document dbObject = new Document();
                int id = rs.getInt("id");
                String genre = rs.getString("genre");
                dbObject.put("_id", id);
                dbObject.put("genre", genre);
                docs.add(dbObject);
            }
            System.out.println("fetched everything");
            genres.insertMany(docs);
            newConn1.commit();
        } catch (SQLException e) {
            // if database exists
            System.out.println(e.getMessage());
            System.out.println("rolling back");
            newConn1.rollback();
        }
    }
    public static void fetchFromDirectedBy(MongoClient mongoClient, MongoDatabase db, Connection newConn1) throws SQLException {
        MongoCollection directedby = db.getCollection("directedby");
        Statement statement = newConn1.createStatement();
        List<Document> docs = new ArrayList<>();
        try {
            newConn1.setAutoCommit(false);
            String str = "SELECT * FROM directedby";
            statement.setFetchSize(20000);
            ResultSet rs = statement.executeQuery(str);
            int count = 0;
            while (rs.next()) {
                Document dbObject = new Document();
                int movieid = rs.getInt("movieid");
                int directorid = rs.getInt("directorid");
                dbObject.put("movieid", movieid);
                dbObject.put("directorid", directorid);
                count++;
                docs.add(dbObject);
            }
            System.out.println("fetched everything");
            directedby.insertMany(docs);
            newConn1.commit();
        } catch (SQLException e) {
            // if database exists
            System.out.println(e.getMessage());
            System.out.println("rolling back");
            newConn1.rollback();
        }
    }
    public static void fetchFromMovieGenres(MongoClient mongoClient, MongoDatabase db, Connection newConn1) throws SQLException {
        MongoCollection moviegenres = db.getCollection("moviegenres");
        Statement statement = newConn1.createStatement();
        List<Document> docs = new ArrayList<>();
        try {
            newConn1.setAutoCommit(false);
            String str = "SELECT * FROM moviegenres";
            statement.setFetchSize(20000);
            ResultSet rs = statement.executeQuery(str);
            int count = 0;
            while (rs.next()) {
                Document dbObject = new Document();
                int genreid = rs.getInt("genreid");
                int movieid = rs.getInt("movieid");
                dbObject.put("genreid", genreid);
                dbObject.put("movieid", movieid);
                count++;
                docs.add(dbObject);
            }
            System.out.println("fetched everything");
            moviegenres.insertMany(docs);
            newConn1.commit();
        } catch (SQLException e) {
            // if database exists
            System.out.println(e.getMessage());
            System.out.println("rolling back");
            newConn1.rollback();
        }
    }
    public static void fetchFromRoles(MongoClient mongoClient, MongoDatabase db, Connection newConn1) throws SQLException {
        MongoCollection roles = db.getCollection("roles");
        Statement statement = newConn1.createStatement();
        List<Document> docs = new ArrayList<>();
        try {
            newConn1.setAutoCommit(false);
            String str = "SELECT * FROM roles";
            statement.setFetchSize(20000);
            ResultSet rs = statement.executeQuery(str);
            while (rs.next()) {
                Document dbObject = new Document();
                int movieid = rs.getInt("movieid");
                int actorid = rs.getInt("actorid");
                String role = rs.getString("role");
                dbObject.put("movieid", movieid);
                dbObject.put("actorid", actorid);
                dbObject.put("role", role);
                //docs.add(dbObject);
                roles.insertOne(dbObject );
            }
            System.out.println("fetched everything");
            //roles.insertMany(docs);
            newConn1.commit();
        } catch (SQLException e) {
            // if database exists
            System.out.println(e.getMessage());
            System.out.println("rolling back");
            newConn1.rollback();
        }
    }

    public static void clearAllTheCollections(MongoDatabase db) {
        BasicDBObject basicDBObject = new BasicDBObject();
        MongoIterable<String> colls = db.listCollectionNames();

        for (String collection : colls) {
            MongoCollection col = db.getCollection(collection);
            col.deleteMany(basicDBObject);
        }
    }
    public static void main(String[] args) throws SQLException {
        MongoClient mongoClient = getMongoConnection();
        MongoDatabase db = createMongoDB(mongoClient);

        Connection newConn1 = getConnection();
        clearAllTheCollections(db);
        System.out.println("fetching actors");
        fetchFromActors(mongoClient, db);
        System.out.println("fetching from directors");
        fetchFromDirectors(mongoClient, db, newConn1);
        System.out.println("fetching from movies");
        fetchFromMovies(mongoClient, db, newConn1);
        System.out.println("fetching from genres");
        fetchFromgenres(mongoClient, db, newConn1);
        System.out.println("fetching from directedby");
        fetchFromDirectedBy(mongoClient, db, newConn1);
        System.out.println("fetching from moviegenre");
        fetchFromMovieGenres(mongoClient, db, newConn1);
        System.out.println("fetching from roles");
        fetchFromRoles(mongoClient, db, newConn1);
        //clearAllTheCollections(db);
        System.out.println("fetching done");
        mongoClient.close();

    }

}

