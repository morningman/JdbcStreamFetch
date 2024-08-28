import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TrinoTest {
    public static void main(String[] args) {
        // Replace with the appropriate values for your Trino server
        String user = "admin";
        String password = "";

        // Load the Trino JDBC driver
        try {
            Class.forName("io.trino.jdbc.TrinoDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }

        // Establish a connection to the Trino database
        try (Connection conn = DriverManager.getConnection(
                "jdbc:trino://127.0.0.1:8080/mysql/test", user, password)) {

            // Create a statement
            Statement statement = conn.createStatement();
            // Set the fetch size to 1000 rows
            statement.setFetchSize(10000);

            // Execute the query
            ResultSet rs = statement.executeQuery("select * from mysql.test.test limit 100000");
            int rowCount = 0;
            while (rs.next()) {
                // Process each row (for demonstration, we're just counting rows)
                rowCount++;

                if (rowCount % 10000 == 0) {
                    System.out.println("Fetched " + rowCount + " rows. Waiting for next fetch...");

                    // Wait for 10 seconds before fetching the next set of rows
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            System.out.println("Total rows fetched: " + rowCount);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
