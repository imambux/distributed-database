package fdbs.fjdbc;

public interface FedPseudoDriverInterface {

    FedConnection getConnection(String username, String password) throws FedException;

}
