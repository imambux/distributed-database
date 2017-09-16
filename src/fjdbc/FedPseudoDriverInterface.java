package fjdbc;

public interface FedPseudoDriverInterface {

    public FedConnection getConnection(String username, String password) throws FedException;

}
