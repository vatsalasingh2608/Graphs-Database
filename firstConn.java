/*
Author: Vatsala Singh
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class firstConn {
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

    public static void main(String[] args) throws SQLException {

        Connection newConn = null;
        PreparedStatement preparedStatementInsert1 = null;
        PreparedStatement preparedStatementInsert2 = null;
        PreparedStatement preparedStatementInsert3 = null;
        PreparedStatement preparedStatementInsert4 = null;
        PreparedStatement preparedStatementInsert5 = null;

        String insertTableSQL = "INSERT INTO actors"
                + "(id, first, last, gender) VALUES"
                + "(?,?,?,?)";
        try {
            newConn = getConnection();
            newConn.setAutoCommit(false);
            // first insert statement
            preparedStatementInsert1 = newConn.prepareStatement(insertTableSQL);
            preparedStatementInsert1.setInt(1, 2);
            preparedStatementInsert1.setString(2, "vatsala");
            preparedStatementInsert1.setString(3, "singh");
            preparedStatementInsert1.setString(4, String.valueOf('F'));
            preparedStatementInsert1.executeUpdate();
            //2nd insert statement
            preparedStatementInsert2 = newConn.prepareStatement(insertTableSQL);
            preparedStatementInsert2.setInt(1, 3);
            preparedStatementInsert2.setString(2, "naina");
            preparedStatementInsert2.setString(3, "singh");
            preparedStatementInsert2.setString(4, String.valueOf('F'));
            preparedStatementInsert2.executeUpdate();
            // 3rd insert statement
            preparedStatementInsert3 = newConn.prepareStatement(insertTableSQL);
            preparedStatementInsert3.setString(1, "test");
            preparedStatementInsert3.setInt(2, 5);
            preparedStatementInsert3.setString(3, "naina");
            preparedStatementInsert3.setString(4, String.valueOf('M'));
            preparedStatementInsert3.executeUpdate();
            // 4th insert statement
            preparedStatementInsert2 = newConn.prepareStatement(insertTableSQL);
            preparedStatementInsert2.setInt(1, 6);
            preparedStatementInsert2.setString(2, "shahrukh");
            preparedStatementInsert2.setString(3, "khan");
            preparedStatementInsert2.setString(4, String.valueOf('M'));
            preparedStatementInsert2.executeUpdate();
            newConn.commit();
        } catch (SQLException e) {

            System.out.println(e.getMessage());
            System.out.println("rolling back");
            newConn.rollback();
            System.out.println("roll back done");

        }
    }
}
