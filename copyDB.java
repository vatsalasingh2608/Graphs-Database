/*
Author: Vatsala Singh
 */
import java.sql.*;

public class copyDB {
    private static String DB_URL = "jdbc:mysql://localhost:3306/";
    private static String DB2_URL = "jdbc:mysql://localhost:3306/imdb";
    private static String imdb2_URL = "jdbc:mysql://localhost:3306/imdb2";
    private static String DB_user = "root";
    private static String DB_passwd = "7542";

    public static Connection getConnection1() throws SQLException {
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
    public static Connection getConnectionimdb2() throws SQLException {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Connection conn = DriverManager.getConnection(imdb2_URL, DB_user, DB_passwd);
        System.err.println("Connection established");
        return conn;
    }


    public static Connection getConnection2() throws SQLException {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Connection conn = DriverManager.getConnection(DB2_URL, DB_user, DB_passwd);
        System.err.println("Connection established");
        return conn;
    }

    public static void main(String[] args) throws SQLException {
        Connection newConn1 = null;
        newConn1 = getConnection1();
        Connection conToImdb = getConnection2();
        PreparedStatement preparedStatementCreateDB = null;
        Statement stmt1 = newConn1.createStatement();
        try {
            stmt1.executeUpdate("DROP DATABASE IF EXISTS imdb2");

            String sql = "CREATE DATABASE imdb2";
            stmt1.executeUpdate(sql);

        } catch (SQLException e) {
            // TODO Auto-generated catch block
            System.out.println("Create Database: " + e);
        }
        System.out.println("db created");
        newConn1 = getConnectionimdb2();
        Statement Stmt = newConn1.createStatement();
        try {
            newConn1.setAutoCommit(false);
            Stmt.executeUpdate("CREATE TABLE Person(id INT (11) NOT NULL, name VARCHAR (250), PRIMARY KEY (id))ENGINE=InnoDB");
            Stmt.executeUpdate("CREATE TABLE Movie (movie_id INT (11)" +
                    " NOT NULL, title VARCHAR (250), release_year INT (11), PRIMARY KEY (movie_id))ENGINE=InnoDB");
            Stmt.executeUpdate("CREATE TABLE Actor(person_id INT (11) NOT NULL, movie_id INT (11) NOT NULL , " +
                    " PRIMARY KEY (person_id, movie_id), FOREIGN KEY (person_id) REFERENCES Person(id)," +
                    "FOREIGN KEY (movie_id) REFERENCES Movie(movie_id))ENGINE=InnoDB");
            Stmt.executeUpdate("CREATE TABLE Director(person_id INT (11) NOT NULL, movie_id INT (11) NOT NULL ," +
                    "PRIMARY KEY (person_id, movie_id), FOREIGN KEY (person_id) REFERENCES Person(id)," +
                    "FOREIGN KEY (movie_id) REFERENCES Movie(movie_id)ENGINE=InnoDB");

            newConn1.commit(); // now the database physically exists
        } catch (SQLException e) {
            // if database exists
            System.out.println(e.getMessage());
            System.out.println("rolling back");
            newConn1.rollback();
        }
        System.out.println("schema created");

        // create the new schema
        //dump actors to person
        // dump roles to actors
        // dump movies to movies
        // run a query in the old database to find out how many ids of directors and actors are common.
        // save the common ids in a hashmap
        // once everything else is dumped in person, start dumping ids from director table
        // but only those which are not there in the common ids
        // once everything is added, create new ids and add it in the person table.
        // map old ids to new ones in a hashmap so that while dumping directedby in director, new id can be added.
        dumpingActorsToPerson(conToImdb, newConn1);
        System.out.println("actor to person done");
        dumpingMoviesToMovie(conToImdb, newConn1);
        System.out.println("movie to movie done");
        dumpingRoleToActor(conToImdb, newConn1);
        System.out.println("role to actor done");
        getCommonActorDirectorIDs(conToImdb);



    }

    public static void dumpingActorsToPerson(Connection conToImdb, Connection conToImdb2) throws SQLException {

        PreparedStatement pp = null;
        String query = "INSERT INTO imdb2.Person(id, name) " +
                "SELECT id, CONCAT_WS(' ', first, last) FROM imdb.actors";
        try {
            conToImdb2.setAutoCommit(false);
            pp = conToImdb2.prepareStatement(query);
            pp.executeUpdate();
            conToImdb2.commit();

        } catch (SQLException e) {

            System.out.println(e.getMessage());
            System.out.println("rolling back");
            conToImdb2.rollback();
            System.out.println("roll back done");
        }
    }
    public static void dumpingMoviesToMovie(Connection conToImdb, Connection conToImdb2) throws SQLException{
        PreparedStatement pp = null;
        String query = "INSERT INTO imdb2.Movie(movie_id, title, release_year) " +
                "SELECT id, title, year FROM imdb.movies";
        try {
            conToImdb2.setAutoCommit(false);
            pp = conToImdb2.prepareStatement(query);
            pp.executeUpdate();
            conToImdb2.commit();

        } catch (SQLException e) {

            System.out.println(e.getMessage());
            System.out.println("rolling back");
            conToImdb2.rollback();
            System.out.println("roll back done");
        }
    }

    public static void dumpingRoleToActor(Connection conToImdb, Connection conToImdb2) throws SQLException {

        PreparedStatement pp = null;
        String query = "INSERT INTO imdb2.Actor(person_id, movie_id) " +
                "SELECT actorid, movieid FROM imdb.roles";
        try {
            conToImdb2.setAutoCommit(false);
            pp = conToImdb2.prepareStatement(query);
            pp.executeUpdate();
            conToImdb2.commit();

        } catch (SQLException e) {

            System.out.println(e.getMessage());
            System.out.println("rolling back");
            conToImdb2.rollback();
            System.out.println("roll back done");
        }
    }
    public static void getCommonActorDirectorIDs(Connection conToImdb) throws SQLException{
        PreparedStatement pp = null;
        conToImdb = getConnection2();
        String query = "SELECT id FROM actors, directors WHERE actors.id = directors.id";

    }
}
