package fdbs;

import application.ApplicationConstants;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


/*
 * 
 * THIS CLASS NOT USED ANYWHERE, BUT CAN BE/SEEMS USEFUL. CAN DELETE IF NOT USED AFTER ALL REQ. ARE COMPLETED
 * 
 */
public class ConnectionFactory {

    static {
        try {
            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection(String connectionURL) {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(connectionURL, ApplicationConstants.USERNAME,
                    ApplicationConstants.PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }
}
