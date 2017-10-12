package fdbs.fjdbc;

public interface FedResultSetInterface {

    boolean next() throws FedException;

    String getString(int columnIndex) throws FedException;

    int getInt(int columnIndex) throws FedException;

    int getColumnCount() throws FedException;

    String getColumnName(int index) throws FedException;

    String getColumnType(int index) throws FedException;

    void close() throws FedException;

}
