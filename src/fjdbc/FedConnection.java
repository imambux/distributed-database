package fjdbc;

import fdbs.CustomLogger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.logging.Level;

public class FedConnection implements FedConnectionInterface {

    private HashMap<Integer, Connection> connectionsMap;

    private boolean autoCommit = true;
    private boolean close = true;

    private FedStatement statement;

    private String userName;
    private String password;

    public FedConnection(HashMap<Integer, Connection> connectionsMap, String userName, String password) {
        this.connectionsMap = connectionsMap;
        close = false;

        // FedConnection has to return FedStatement, so initializing it as soon
        // as FedConnection object is created
        initializeFedStatement();
    }

    private void initializeFedStatement() {
        HashMap<Integer, Statement> statements = new HashMap<Integer, Statement>();

        // Creating JDBC Statements from JDBC Connection Map to prepare
        // FedStatement object
        int i = 0;
        for (Connection connection : connectionsMap.values()) {
            try {
                statements.put(++i, connection.createStatement());
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        statement = new FedStatement(this, statements);
    }

    public boolean getAutoCommit() throws FedException {
        if (close) {
            throw new FedException(
                    new Throwable("FedConnection resource is closed."));
        }

        return autoCommit;
    }

    public void setAutoCommit(boolean commit) throws FedException {
        if (close) {
            throw new FedException(
                    new Throwable("FedConnection resource is closed."));
        }

        try {
            for (Connection connection : connectionsMap.values()) {
                connection.setAutoCommit(commit);
            }
            autoCommit = commit;
        } catch (SQLException e) {
            throw new FedException(new Throwable(e.getMessage()));
        }
    }

    public void commit() throws FedException {
        CustomLogger.log(Level.INFO, "FJDBC: commit()");
        if (close) {
            throw new FedException(
                    new Throwable("FedConnection resource is closed."));
        }
        try {
            for (Connection connection : connectionsMap.values()) {
                connection.commit();
            }
        } catch (SQLException e) {
            throw new FedException(new Throwable(e.getMessage()));
        }
    }

    public void rollback() throws FedException {
        CustomLogger.log(Level.INFO, "FJDBC: rollback()");
        if (close) {
            throw new FedException(
                    new Throwable("FedConnection resource is closed."));
        }

        try {
            for (Connection connection : connectionsMap.values()) {
                connection.rollback();
            }
        } catch (SQLException e) {
            throw new FedException(new Throwable(e.getMessage()));
        }
    }

    public void close() throws FedException {
        CustomLogger.log(Level.INFO, "Entered FedConnection.close()");
        String url;
        try {
            for (Connection connection : connectionsMap.values()) {
                url = connection.getMetaData().getURL();
                connection.close();
                CustomLogger.log(Level.INFO, "JDBC connection to " + url + " has been closed.");
            }
            close = true;
        } catch (SQLException e) {
            throw new FedException(new Throwable(e.getMessage()));
        }
        CustomLogger.log(Level.INFO, "Exited FedConnection.close()");
    }

    public FedStatement getStatement() {
        return statement;
    }

}
