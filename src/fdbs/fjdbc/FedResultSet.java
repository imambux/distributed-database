package fdbs.fjdbc;

import fdbs.logging.CustomLogger;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Level;

/*
 * CAUTION: Haven't implemented this class yet, might/might not need modificaions. So can't comment on it. Can be changed based on requirements
 */
public class FedResultSet implements FedResultSetInterface {
    private List<ResultSet> resultSets;
    private ResultSet currentResultSet;
    private boolean isClose = true;
    private String fcrClosed = "FedConnection resource is closed. ";

    public FedResultSet(List<ResultSet> resultSets) {
        this.resultSets = resultSets;
        currentResultSet = resultSets.remove(0);
        isClose = false;
    }

    public boolean next() throws FedException {
        if (isClose) {
            CustomLogger.log(Level.WARNING, fcrClosed);
            throw new FedException(new Throwable(fcrClosed));
        }

        boolean hasNext = false;
        try {
            hasNext = currentResultSet.next();
        } catch (SQLException e) {
            CustomLogger.log(Level.WARNING, fcrClosed + e.getLocalizedMessage());
            throw new FedException(new Throwable(e.getMessage()));
        }
        if (hasNext) {
            return true;
        }
        if (resultSets.isEmpty()) {
            return false;
        }

        currentResultSet = resultSets.remove(0);
        try {
            hasNext = currentResultSet.next();
        } catch (SQLException e) {
            CustomLogger.log(Level.WARNING, e.getLocalizedMessage());
            return false;
        }
        return hasNext;
    }

    public String getString(int columnIndex) throws FedException {
        if (isClose) {
            CustomLogger.log(Level.WARNING, "FedException; " + fcrClosed);
            throw new FedException(new Throwable(fcrClosed));
        }
        String value = null;
        try {
            value = currentResultSet.getString(columnIndex);
        } catch (SQLException e) {
            CustomLogger.log(Level.WARNING, e.getLocalizedMessage());
            //why throw in catch?
            throw new FedException(new Throwable(e.getMessage()));
        }
        return value;
    }

    public int getInt(int columnIndex) throws FedException {
        if (isClose) {
            CustomLogger.log(Level.WARNING, "FedException; " + fcrClosed);
            throw new FedException(new Throwable(fcrClosed));
        }
        Integer value = null;
        try {
            value = currentResultSet.getInt(columnIndex);
        } catch (SQLException e) {
            CustomLogger.log(Level.WARNING,
                    "SQLException; " + e.getLocalizedMessage());
            throw new FedException(new Throwable());
        }
        return value.intValue();
    }

    public int getColumnCount() throws FedException {
        if (isClose) {
            CustomLogger.log(Level.WARNING, "FedException; " + fcrClosed);
            throw new FedException(new Throwable(fcrClosed));
        }
        int value = 0;
        try {
            value = currentResultSet.getMetaData().getColumnCount();
        } catch (SQLException e) {
            CustomLogger.log(Level.WARNING,
                    "SQLException; " + e.getLocalizedMessage());
            throw new FedException(new Throwable(e.getMessage()));
        }
        return value;
    }

    public String getColumnName(int index) throws FedException {
        if (isClose) {
            CustomLogger.log(Level.WARNING, "FedException; " + fcrClosed);
            throw new FedException(
                    new Throwable("FedConnection resource is closed."));
        }
        String value = "";
        try {
            value = currentResultSet.getMetaData().getColumnName(index);
        } catch (SQLException e) {
            CustomLogger.log(Level.WARNING,
                    "SQLException; " + e.getLocalizedMessage());
            throw new FedException(new Throwable(e.getMessage()));
        }
        return value;
    }

    public String getColumnType(int index) throws FedException {
        if (isClose) {
            throw new FedException(
                    new Throwable("FedConnection resource is closed."));
        }
        //Integer value = null;
        String v = null;
        try {
            ResultSetMetaData rsmd = currentResultSet.getMetaData();
            v = rsmd.getColumnTypeName(index).replaceAll("[0-9]", "");
            //System.out.println(rsmd.getColumnTypeName(index)+"\t\t\t"+rsmd.getColumnName(index));
            //System.out.println(v);
        } catch (SQLException e) {
            throw new FedException(new Throwable(e.getMessage()));
        }
        return v;
    }

    public void close() throws FedException {
        try {
            currentResultSet.close();
            isClose = true;
        } catch (SQLException e) {
            throw new FedException(new Throwable(e.getMessage()));
        }
    }

}
