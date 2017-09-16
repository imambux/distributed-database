package hsfulda.de;

import fjdbc.FedConnection;
import fjdbc.FedException;
import fjdbc.FedPseudoDriver;

/**
 * Comment by: Imam Bux
 * This is the application layer provided by Professor which will test our implementation of FJDC
 */
public class App {
    public static void main(String[] args) throws FedException {

        // Username and Passwords are specific to every student, this is mine :: Imam Bux
        String usernameTest = "VDBSA05";
        String passwordTest = "VDBSA05";

        String usernameValidation = "VDBSA05";
        String passwordValidation = "VDBSA05";

        FedConnection fedConnection = null;

    /*
     * Test schema
     */
        try {
            fedConnection = (new FedPseudoDriver()).getConnection(usernameValidation,
                    passwordValidation);

            FedTestEnvironment fedTestEvironment = new FedTestEnvironment(
                    fedConnection);

            fedTestEvironment.run("Test/DRPTABS.SQL", true);

            fedTestEvironment.run("Test/CREPARTABS.SQL", true);
            fedTestEvironment.run("Test/INSERTAIRPORTS.SQL", true);
            fedTestEvironment.run("Test/INSERTAIRLINES.SQL", true);
            fedTestEvironment.run("Test/INSERTPASSENGERS.SQL", true);
            fedTestEvironment.run("Test/INSERTFLIGHTS.SQL", true);
            fedTestEvironment.run("Test/INSERTBOOKINGS.SQL", true);
            fedTestEvironment.run("Test/PARSELCNTSTAR.SQL", true);

            fedTestEvironment.run("Test/PARSELS1T.SQL", true);
            fedTestEvironment.run("Test/PARSELS1OR.SQL", true);
            fedTestEvironment.run("Test/PARSELSJOIN1.SQL", true);
            fedTestEvironment.run("Test/PARSELS1TGP.SQL", true);
            fedTestEvironment.run("Test/PARSELS1TWGP.SQL", true);   //OPTIONAL
            fedTestEvironment.run("Test/PARSELS1TGHAV.SQL", true);  //OPTIONAL
            fedTestEvironment.run("Test/PARUPDS.SQL", true);
            fedTestEvironment.run("Test/PARINSERTS.SQL", true);
            fedTestEvironment.run("Test/PARDELS.SQL", true);
            fedTestEvironment.run("Test/PARSELCNTSTAR.SQL", true);
        } catch (FedException fedException) {
            fedException.printStackTrace();
        } finally {
            fedConnection.close();
        }

    }
}
