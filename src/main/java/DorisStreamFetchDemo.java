import com.mysql.cj.jdbc.StatementImpl;
import io.trino.jdbc.$internal.guava.util.concurrent.MoreExecutors;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class DorisStreamFetchDemo {

    public static final String USER = "root";
    public static final String PASSWORD = "";
    public static final String HOST_PORT = "172.20.32.136:8833";
    public static final String DB = "dlf.tpcds100_oss";
    public static final int MAX_FETCH_ROW = 1000;

    public static void main(String[] args) {
        // Load the Doris JDBC driver
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }

        String sql  = "select * from inventory";
        doStreamFetch(sql);
    }

    private static void doStreamFetch(String sql) {
        String jdbcUrl = "jdbc:mysql://" + HOST_PORT + "/" + DB + "?useCursorFetch=true";
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = DriverManager.getConnection(jdbcUrl, USER, PASSWORD);
            // 1. get current connection id
            int connectionId = getConnectionId(connection);

            // 2. execute query
            statement = connection.createStatement();
            // 2.1 enable streaming fetch
            ((StatementImpl) statement).enableStreamingResults();
            statement.execute(sql);
            resultSet = statement.getResultSet();
            // 2.2 only fetch MAX_FETCH_ROW rows
            int rowCount = 0;
            while (resultSet.next()) {
                rowCount++;
                if (rowCount > MAX_FETCH_ROW) {
                    break;
                }
            }

            // 3. kill current query
            killQuery(connectionId);
            System.out.println("Total rows fetched: " + rowCount);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (resultSet != null) resultSet.close();
                if (statement != null) statement.close();
                if (connection != null) connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // Get id of current connection
    private static int getConnectionId(Connection conn) {
        try (Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT CONNECTION_ID();");) {
            int connectionId = -1;
            if (resultSet.next()) {
                connectionId = resultSet.getInt(1);
                System.out.println("Current connection id: " + connectionId);
            }
            return connectionId;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // User another connection to kill the query
    private static void killQuery(long id) {
        String jdbcUrl = "jdbc:mysql://" + HOST_PORT + "/" + DB + "?useCursorFetch=true";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, USER, PASSWORD);
                Statement killStatement = conn.createStatement()) {
            String killQuery = "KILL QUERY " + id;
            killStatement.execute(killQuery);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
