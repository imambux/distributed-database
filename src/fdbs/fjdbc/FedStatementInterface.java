package fdbs.fjdbc;

public interface FedStatementInterface {

    int executeUpdate(String sql) throws FedException;

    FedResultSet executeQuery(String sql) throws FedException;

    FedConnection getConnection() throws FedException;

    void close() throws FedException;

}
