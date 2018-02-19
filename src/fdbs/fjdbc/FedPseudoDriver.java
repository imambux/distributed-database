package fdbs.fjdbc;

import fdbs.ConnectionConstants;
import fdbs.logging.CustomLogger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.logging.Level;

public class FedPseudoDriver implements FedPseudoDriverInterface {

    // Gets FedConnection. FedConnection has 3 Database Connections in a Map
    public FedConnection getConnection(String userName, String password)
            throws FedException {
        CustomLogger.log(Level.INFO, "Start FDBS");
        FedConnection connection = null;
        try {
            HashMap<Integer, Connection> connectionsMap = new HashMap<Integer, Connection>();

            connectionsMap.put(1, DriverManager.getConnection(
                    ConnectionConstants.CONNECTION_1_URL, userName, password));
            //String info = "FDBS; opened connection to ";
            //CustomLogger.log(Level.INFO, info + ConnectionConstants.CONNECTION_1_SID);
            connectionsMap.put(2, DriverManager.getConnection(
                    ConnectionConstants.CONNECTION_2_URL, userName, password));
            //CustomLogger.log(Level.INFO, info + ConnectionConstants.CONNECTION_2_SID);
            connectionsMap.put(3, DriverManager.getConnection(
                    ConnectionConstants.CONNECTION_3_URL, userName, password));
            //CustomLogger.log(Level.INFO, info + ConnectionConstants.CONNECTION_3_SID);

            connection = new FedConnection(connectionsMap, userName, password);
        } catch (SQLException e) {
            throw new FedException(new Throwable(e.getMessage()));
        }
        return connection;
    }

}
