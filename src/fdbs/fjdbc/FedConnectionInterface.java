package fdbs.fjdbc;

public interface FedConnectionInterface {

    boolean getAutoCommit() throws FedException;

    void setAutoCommit(boolean autoCommit) throws FedException;

    void commit() throws FedException;

    void rollback() throws FedException;

    void close() throws FedException;

    FedStatement getStatement();
}
