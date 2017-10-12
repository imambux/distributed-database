package fdbs.fjdbc;

import fdbs.DatabaseCatalog;
import fdbs.FederalController;
import fdbs.logging.CustomLogger;
import fdbs.parser.ParseException;

import java.sql.Statement;
import java.util.HashMap;
import java.util.logging.Level;

public class FedStatement implements FedStatementInterface {
    String fsrClosed = "FedStatement resource is closed.";
    // Map of JDBC Statements
    private HashMap<Integer, Statement> statementsMap;

    private FedConnection connection;

    private boolean isClose = true;

    public FedStatement(FedConnection connection,
                        HashMap<Integer, Statement> statementsMap) {
        this.connection = connection;
        this.statementsMap = statementsMap;
        DatabaseCatalog.setStatementsMap(statementsMap);
        isClose = false;

        // Initialize query executor to use JDBC statements
        FederalController.setFedStatement(this);
        FederalController.setStatementsMap(statementsMap);
    }

    public int executeUpdate(String query) throws FedException {
        int result = -1;

        if (query.trim().startsWith("-") || query.trim().startsWith("//") || query.trim().toLowerCase().startsWith("applies ") || query.trim().startsWith("\\") || query.trim().startsWith("#"))
            return result;

        if (isClose) {
            CustomLogger.log(Level.WARNING, fsrClosed);
            throw new FedException(new Throwable(fsrClosed));
        }

        CustomLogger.log(Level.INFO, "Received FJDBC: " + query);
        try {
            result = FederalController.executeUpdate(query);
        } catch (ParseException e) {
            CustomLogger.log(Level.WARNING, "ParseException; " + e);
            throw new FedException(new Throwable(e.getMessage()));
        }
        return result;
    }

    public FedConnection getConnection() throws FedException {
        if (isClose) {
            throw new FedException(new Throwable(fsrClosed));
        }
        return connection;
    }

    public void close() throws FedException {
//        try {
//            // Closes all JDBC Statements
//            for (Statement statement : statementsMap.values()) {
//                statement.close();
//            }
//            isClose = true;
//        } catch (SQLException e) {
//            CustomLogger.log(Level.WARNING, "FedException; " + e.getMessage());
//            throw new FedException(new Throwable(e.getMessage()));
//        }
    }

    public FedResultSet executeQuery(String sql) throws FedException {
        if (sql.trim().startsWith("-") || sql.trim().startsWith("//") || sql.trim().toLowerCase().startsWith("applies ") || sql.trim().startsWith("\\") || sql.trim().startsWith("#"))
            return null;

        CustomLogger.log(Level.INFO, "Received FJDBC: " + sql);
        if (isClose) {
            throw new FedException(new Throwable(fsrClosed));
        }

        FedResultSet instance = FederalController.executeSelectQuery(sql);

        return instance;
    }
}