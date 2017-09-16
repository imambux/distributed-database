package fjdbc;

public interface FedConnectionInterface {

    public boolean getAutoCommit() throws FedException;

    public void setAutoCommit(boolean autoCommit) throws FedException;

    public void commit() throws FedException;

    public void rollback() throws FedException;

    public void close() throws FedException;

    public FedStatement getStatement();
}
