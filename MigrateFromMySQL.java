import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class MigrateFromMySQL {
    private static String DB_URL = "jdbc:mysql://localhost:3306/imdb";
    private static String DB_user = "root";
    private static String DB_passwd = "7542";
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
    public static void deleteFileOrDirectory( final File file ) {
        if ( file.exists() ) {
            if ( file.isDirectory() ) {
                for ( File child : file.listFiles() ) {
                    deleteFileOrDirectory( child );
                }
            }
            file.delete();
        }
    }
    public static void populateAllNodes(BatchInserter batchInserter) throws SQLException {
        Connection newConn1 = getConnection();
        Statement statement = newConn1.createStatement();
        Map<Integer, Long> actorMap = new HashMap<>();
        Map<Integer, Long> movieMap = new HashMap<>();
        Map<Integer, Long> directorMap = new HashMap<>();
        Map<Integer, Long> genreMap = new HashMap<>();
        try {
            newConn1.setAutoCommit(false);
            String str = "SELECT * FROM actors";
            statement.setFetchSize(10000);
            ResultSet rs = statement.executeQuery(str);
            while (rs.next()) {
                Map<String, Object> properties = new HashMap<String, Object>();
                int id = rs.getInt("id");
                String first = rs.getString("first");
                String last = rs.getString("last");
                String gender = rs.getString("gender");
                properties.put("actorid", id);
                properties.put("first", first);
                properties.put("last", last);
                properties.put("gender", gender);
                long actorid = batchInserter.createNode(properties, Label.label("Actor"));
                actorMap.put(id, actorid);

            }
            System.out.println("fetched from actors everything");
            String str1 = "SELECT * FROM movies";
            statement.setFetchSize(10000);
            ResultSet rs1 = statement.executeQuery(str1);
            while (rs1.next()) {
                Map<String, Object> properties = new HashMap<String, Object>();
                int id = rs1.getInt("id");
                String title = rs1.getString("title");
                int year = rs1.getInt("year");
                properties.put("movieid", id);
                properties.put("title", title);
                properties.put("year", year);
                long movieid = batchInserter.createNode(properties, Label.label("Movie"));
                movieMap.put(id, movieid);
            }
            System.out.println("fetched from movies everything");
            String str2 = "SELECT * FROM roles";
            statement.setFetchSize(10000);
            ResultSet rs2 = statement.executeQuery(str2);
            while (rs2.next()) {
                Map<String, Object> properties = new HashMap<>();
                int actorid = rs2.getInt("actorid");
                int movieid = rs2.getInt("movieid");
                String role = rs2.getString("role");
                if (role == null) {
                    properties = null;
                }
                else {
                    properties.put("role", role);
                }
                if (movieMap.containsKey(movieid) && actorMap.containsKey(actorid)) {
                    long actorNode = actorMap.get(actorid);
                    long movieNode = movieMap.get(movieid);
                    if( batchInserter.nodeExists(actorNode) && batchInserter.nodeExists(movieNode)) {
                        batchInserter.createRelationship(actorNode, movieNode, RelationshipType.withName("acted_in"), properties);
                    }
                }
            }
            System.out.println("created actor role relationships");
            String str3 = "SELECT * FROM directors";
            statement.setFetchSize(10000);
            ResultSet rs3 = statement.executeQuery(str3);
            while (rs3.next()) {
                Map<String, Object> properties = new HashMap<>();
                int id = rs3.getInt("id");
                String first = rs3.getString("first");
                String last = rs3.getString("last");
                properties.put("directorid", id);
                properties.put("first", first);
                properties.put("last", last);
                long directorid = batchInserter.createNode(properties, Label.label("Director"));
                directorMap.put(id, directorid);

            }
            String str4 = "SELECT * FROM directedby";
            statement.setFetchSize(10000);
            ResultSet rs4 = statement.executeQuery(str4);
            while (rs4.next()) {
                int directorid = rs4.getInt("directorid");
                int movieid = rs4.getInt("movieid");
                if (movieMap.containsKey(movieid) && directorMap.containsKey(directorid)) {
                    long directorNode = directorMap.get(directorid);
                    long movieNode = movieMap.get(movieid);
                    if( batchInserter.nodeExists(directorNode) && batchInserter.nodeExists(movieNode)) {
                        batchInserter.createRelationship(movieNode, directorNode, RelationshipType.withName("director_by"), null);
                    }
                }
            }
            String str5 = "SELECT * FROM genres";
            statement.setFetchSize(10000);
            ResultSet rs5 = statement.executeQuery(str5);
            while (rs5.next()) {
                Map<String, Object> properties = new HashMap<>();
                int id = rs5.getInt("id");
                String genre = rs5.getString("genre");
                properties.put("genreid", id);
                properties.put("genre", genre);
                long genreid = batchInserter.createNode(properties, Label.label("Genre"));
                genreMap.put(id, genreid);

            }
            String str6 = "SELECT * FROM moviegenres";
            statement.setFetchSize(10000);
            ResultSet rs6 = statement.executeQuery(str6);
            while (rs6.next()) {
                int movieid = rs6.getInt("movieid");
                int genreid = rs6.getInt("genreid");
                if (movieMap.containsKey(movieid) && genreMap.containsKey(genreid)) {
                    System.out.println("found the match");
                    long genreNode = genreMap.get(genreid);
                    long movieNode = movieMap.get(movieid);
                    if( batchInserter.nodeExists(genreNode) && batchInserter.nodeExists(movieNode)) {
                        batchInserter.createRelationship(movieNode, genreNode, RelationshipType.withName("of_genre"),null);
                    }
                }
            }
            batchInserter.shutdown();
            newConn1.commit();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            newConn1.rollback();
        }
    }

    public static void main(String[] args) throws IOException, SQLException {
        deleteFileOrDirectory(new File("C:/imdb"));
        BatchInserter batchInserter1 = BatchInserters.inserter(new File("C:/imdb"));
        populateAllNodes(batchInserter1);
    }
}
