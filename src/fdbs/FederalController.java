/* Taken from [11] Dennis McLeod and Dennis Heimbigner.
 * A federated architecture for database systems. In Proceedings of
 * the May 19-22, 1980, national computer conference, AFIPS &rsquo;80.
 * Federal schema:
 * - describes the data that is to be shared by the various federation components,
 * - provides a common basis for communication among them.
 * A federal controller supports communication and translation of data among
 * federation components, based on the federal schema.
 *
 * The federal controller [FC] is a control component that performs the bulk of
 * the transformations necessary to satisfy a request for information described
  * in a federal schema (and that is contained in another component).
  * The request takes the form of a specified transaction.
 *
 * The FC must perform a sequence of seven steps for each request/transaction:
 * [we will not focus on text in brackets]
 * 1. The transaction is checked for legality against the federal schema.
 * [The access rights of the requester are also  verified at this time].
 * @author Anfilov: an opportunity here is to discard a query received via FJBDC
 * if the query contains a table name that does not exist in a federal schema.
 *
 * 2. The transaction is decomposed into a collection of simpler target
 * transactions, each of which can be ultimately satisfied by a single target
 * component. The target component is the component that supports that part of
 * the federal schema referenced by the target transaction.
 * @author Anfilov: target component is any of 3 databases.
 *
 * 3. Each target transaction is translated from a reference to the federal schema
 * to a reference to the target component schema.
 * @author Anfilov: we have already implemented sending of FJDBS to every database.
 * Now let's focus on implementing the database catalogue for smart query distribution.
 *
 * 4. The target transactions are sent to the corresponding target components for
 * processing.
 * @author Anfilov: see my comment above for step 3.
 *
 * 5. The federal controller waits for all the target transactions to be processed,
 * and then the controller collects the results.
 * @author Anfilov: the results in a form of up to 3 JDBC ResultSets.
 * [6. The results are translated from target schema form back to federal schema form.]
 *
 * 7. The [translated] results are combined and returned to the requester.
 * @author Anfilov: our smart implementation will merge the results from the step 5
 * into a FedResultSet.
 *
 * Steps 5 - 7 can be performed either set-at-a-time or element-at-a-time.
 * Set-at-a-time: the FC collects the results from all of the target components
 * into a single result set, which is then returned to the requester.
 *
 * Element-at-a-time: the FC translates and returns to the requester each element
 * of the result as it is made available by a target component.
 *
 * The choice between set-at-a-time and element-at-a-time should be made based on
 * storage cost and communication cost information.
 */

package fdbs;

import fdbs.fjdbc.FedException;
import fdbs.fjdbc.FedResultSet;
import fdbs.fjdbc.FedStatement;
import fdbs.logging.CustomLogger;
import fdbs.parser.GepardParser;
import fdbs.parser.ParseException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FederalController {

    private static FedStatement fedStatement;
    /*
     * This map holds the =DBC Statement, it it initialized automatically when
     * FedStatement is initialized.
     */
    private static HashMap<Integer, Statement> statementsMap;

    public static void setStatementsMap(HashMap<Integer, Statement> statements) {
        statementsMap = statements;
    }

    /*
     * Can be used to execute SET command or some other commands like this. Not
     * sure about it's usefullness at the moment.
     */
    public static void execute(String query) {

    }

    public static int executeUpdate(String query)
            throws FedException, ParseException {
        int queryType = QueryTypeConstant.NONE;
        int result = -1;

    /* Some complex preprocess start */

    /*
    * Removes tabs, extra spaces and lines for fdbs.parser to understand according
     * to the grammar. NOTE: We use this method because skipping tabs, spaces
     * and new lines does not work efficiently.
     */
        boolean isInsertQuery = query.toUpperCase().startsWith("INSERT");
        String preserveWhereClause = "";
        if (isInsertQuery) {
            preserveWhereClause = query.substring(query.indexOf("VALUES") + 6);
            query = query.substring(0, query.indexOf("VALUES") + 6);
        }

        query = processQueryForParser(query);

    /* Some complex preprocess end */

        // Every query needs ';' to parse, so being added here.
        // Parsing starts here.
        GepardParser parser = new GepardParser(convertToParsableQuery(query + ";"));

        // This method is a general method from where all grammar starts.
        queryType = parser.ParseQuery();

    /* Some complex post process start before going to database */

    /*
     * Special characters like umlauts were replaced with unicode equivalents
     * while running processQueryForParser method. Reason: JavaCC does not
     * support umlauts
     */
        query = UnicodeManager.replaceUnicodesWithChars(query);
        query = replace3DashesWithSpace(query);
        query = replaceBraces(query);

        if (isInsertQuery) {
            query += preserveWhereClause;
        }
    /* Some complex post process end before going to database */

        switch (queryType) {
            case QueryTypeConstant.CREATE_NON_PARTITIONED:
                result = createTable(query);
                break;
            case QueryTypeConstant.CREATE_PARTITIONED:
                result = createTableHorizontal(query);
                break;
            case QueryTypeConstant.DROP:
                result = dropTable(query);
                break;
            case QueryTypeConstant.DELETE:
                result = deleteFromTable(query);
                break;
            case QueryTypeConstant.INSERT:
                result = insertIntoTable(query);
                break;
            default:
                result = executeDefaultQuery(query);
        }

        return result;
    }

    private static String replaceBraces(String query) {
        query = query.replaceAll("[(]{3}", "(");
        query = query.replaceAll("//////", ")");
        return query;
    }

    /*
     * Was added to parse successfully because I am unable to handle space in
     * between string constant in Parser
     */
    private static String replace3DashesWithSpace(String query) {
        Pattern pattern = Pattern.compile("'(.+[---]+.+)'");
        Matcher m = pattern.matcher(query);
        while (m.find()) {
            String searchStr = m.group();
            query = query.replaceAll(searchStr, searchStr.replaceAll("---", " "));
        }
        return query;
    }

    private static String processQueryForParser(String query) {
        StringBuilder sb = new StringBuilder(query);

        List<Pattern> patterns = new ArrayList<Pattern>();
        patterns.add(Pattern.compile("\t"));
        patterns.add(Pattern.compile("\n"));
        patterns.add(Pattern.compile("\r"));

    /*
     * There are so many doubles spaces, so some of them are not replaced, thus
     * creating problem for fdbs.parser, so we want to check as much as 10 times to
     * make sure the query is finely formatted.
     */
        int counter = 10;
        while (counter > 0) {
            patterns.add(Pattern.compile("  "));
            counter--;
        }

        for (Pattern pattern : patterns) {
            Matcher m = pattern.matcher(sb);
            sb = new StringBuilder(m.replaceAll(" "));
        }

        query = sb.toString();

    /*
     * Removes all trailing spaces in string constant because they are redundant e.g.
     * 'ABC ' become 'ABC'. It is also important to remove because Parser is unable to parse the query with trailing spaces
     */
        Pattern pattern = Pattern.compile("'([^\\s']+)([ ])+'");
        Matcher m = pattern.matcher(query);
        while (m.find()) {
            String searchStr = m.group();
            query = query.replaceAll(searchStr, searchStr.replaceAll(" ", ""));
        }

    /*
     * Replaces spaces in between string constant with ___ (with replace back)
     * e.g. 'ABC XYZ' becomes 'ABC___XYZ'. It is done because I am unable to
     * handle space between string constant fdbs.parser.
     */
        pattern = Pattern.compile("'([^\\s']+)([ ])+([^\\s']+)'");
        m = pattern.matcher(query);
        while (m.find()) {
            String searchStr = m.group();
            query = query.replaceAll(searchStr, searchStr.replaceAll(" ", "---"));
        }

        pattern = Pattern.compile("\\((.*?)[.](.*?)[ ](.*?)\\)\\)?");
    /*
     * Replaces ( and ) because ) is showing conflict in fdbs.parser and can not add
     * it in String constant, so workaround.
     */
        m = pattern.matcher(query);
        while (m.find()) {
            String searchStr = m.group();
            query = query.replace(searchStr,
                    searchStr.replaceAll("\\(", "").replaceAll("\\)", ""));
        }

    /*
     * Replaces umlauts with unicodes to parse successfully because JavaCC
     * replaces umlauts with unicodes too
     */
        query = UnicodeManager.getUnicodedQuery(query);
        return query;
    }

    private static boolean shouldNotParse(String query) {
        return query.startsWith("SET") || query.startsWith("ALTER");
    }

    private static int executeDefaultQuery(String query) throws FedException {
        int result = -1;
        String connectionDB = "";
        Integer connectionNumber = -1;
        Statement statement = null;
        // Simple SET query, i.e. set echo on, will be ignored
        if (query.toUpperCase().startsWith("SET"))
            return 0;
        CustomLogger.log(Level.INFO, "Received FJDBC: " + query);
        try {
            for (Integer statementKey : statementsMap.keySet()) {
                connectionNumber = statementKey;
                if (statementKey == 1) {
                    connectionDB = ConnectionConstants.CONNECTION_1_SID;
                }
                if (statementKey == 2) {
                    connectionDB = ConnectionConstants.CONNECTION_2_SID;
                }
                if (statementKey == 3) {
                    connectionDB = ConnectionConstants.CONNECTION_3_SID;
                }
                statement = statementsMap.get(statementKey);
                CustomLogger.log(Level.INFO,
                        "Sending to " + connectionDB + ": " + query);
                result = statement.executeUpdate(query);
            }
        } catch (SQLException e) {
            String message = "Connect " + connectionNumber + " " + connectionDB + ": "
                    + e.getLocalizedMessage();
            CustomLogger.log(Level.SEVERE, "JDBC SQLException in " + connectionDB
                    + ": " + e.getLocalizedMessage());
            throw new FedException(new Throwable(message));
        }
    /*
     * CREATE query is neither INSERT nor UPDATE so it will always return 0 as
     * it effects 0 tuples
     */
        return 0;
    }

    private static int deleteFromTable(String query) throws FedException {
        int result = 0;
        String connectionDB = "";
        int statementKey = 1;
        Statement statement = null;
        CustomLogger.log(Level.INFO, "Received FJDBC: " + query);
        // Map of 3 oracle.jdbc.driver.OracleStatement objects
        while (statementKey <= statementsMap.size()) {
            statement = statementsMap.get(statementKey);
            if (statementKey == 1) {
                connectionDB = ConnectionConstants.CONNECTION_1_SID;
            }
            if (statementKey == 2) {
                connectionDB = ConnectionConstants.CONNECTION_2_SID;
            }
            if (statementKey == 3) {
                connectionDB = ConnectionConstants.CONNECTION_3_SID;
            }
            CustomLogger.log(Level.INFO, "Sending to " + connectionDB + ": " + query);
            try {
                result += statement.executeUpdate(query);
                statementKey++;
            } catch (SQLException e) {
                if (e instanceof SQLIntegrityConstraintViolationException) {
                    statementKey++;
                    continue;
                } else if (fedStatement.getConnection().getAutoCommit() == false) {
                    fedStatement.getConnection().rollback();

                    String message = "Connect " + statementKey + " " + connectionDB + ": "
                            + e.getLocalizedMessage();
                    CustomLogger.log(Level.SEVERE, "JDBC SQLException in " + connectionDB
                            + ": " + e.getLocalizedMessage());
                    throw new FedException(new Throwable(message));
                }
                e.printStackTrace();
            }

        }
        return result;
    }

    private static int insertIntoTable(String query) throws FedException {
        //        HashMap<Integer, Statement> statements = DatabaseCatalog.getStatementsForQuery(query);

        HashMap<Integer, Statement> statements = statementsMap;

        int result = -1;
        String connectionDB = "";
        int statementKey = 1;

        Statement statement = null;
        // Logger: redundant, was called earlier in executeUpdate
        // CustomLogger.log(Level.INFO, "Received FJDBC: " + query);
        while (statementKey <= statements.size()) {
            statement = statements.get(statementKey);
            if (statementKey == 1) {
                connectionDB = ConnectionConstants.CONNECTION_1_SID;
            }
            if (statementKey == 2) {
                connectionDB = ConnectionConstants.CONNECTION_2_SID;
            }
            if (statementKey == 3) {
                connectionDB = ConnectionConstants.CONNECTION_3_SID;
            }

            try {
                CustomLogger.log(Level.INFO,
                        "Sending to " + connectionDB + ": " + query);
                result = statement.executeUpdate(query);
                statementKey++;
            } catch (SQLException e) {
                if (e instanceof SQLIntegrityConstraintViolationException) {
                    if (e.getMessage().toLowerCase().contains("unique constraint")) {
                        throw new FedException(
                                new Throwable(e.getMessage()));
                    } else if (e.getMessage().toLowerCase().contains("check ") && !e.getMessage().toLowerCase().contains("horiz")) {
                        throw new FedException(
                                new Throwable(e.getMessage()));
                    } else if (e.getMessage().toLowerCase()
                            .contains("integrity constraint")) {
                        disableAllReferentialConstraints(query, statement);
                        statementKey--;
                    }
                    statementKey++;
                    continue;
                } else if (fedStatement.getConnection().getAutoCommit() == false) {
                    fedStatement.getConnection().rollback();
                    String message = "Connect " + statementKey + " " + connectionDB + ": "
                            + e.getLocalizedMessage();
                    CustomLogger.log(Level.SEVERE, "JDBC SQLException in " + connectionDB
                            + ": " + e.getLocalizedMessage());
                    throw new FedException(new Throwable(message));
                }
                e.printStackTrace();
            }
        }
        return result;
    }

    private static void disableAllReferentialConstraints(String query,
                                                         Statement statement) {
        String table = DatabaseCatalog.getTableFromInsertQuery(query);
        DatabaseCatalog.disableAllReferentialConstraints(table, statement);
    }

    private static int createTable(String query) throws FedException {
        String connectionDB = "";
        Integer connectionNumber = -1;
        boolean hasException = false;
        String exceptionMessage = "";

        CustomLogger.log(Level.INFO, "Received FJDBC: " + query);
        Statement statement = null;
        for (Integer statementKey : statementsMap.keySet()) {
            connectionNumber = statementKey;

            if (statementKey == 1) {
                connectionDB = ConnectionConstants.CONNECTION_1_SID;
            }
            if (statementKey == 2) {
                connectionDB = ConnectionConstants.CONNECTION_2_SID;
            }
            if (statementKey == 3) {
                connectionDB = ConnectionConstants.CONNECTION_3_SID;
            }
            statement = statementsMap.get(statementKey);

            try {
                CustomLogger.log(Level.INFO,
                        "Sending to " + connectionDB + ": " + query);
                statement.executeUpdate(query);
            } catch (Exception e) {
                String message = "Connect " + connectionNumber + " " + connectionDB
                        + ": " + e.getLocalizedMessage();
                CustomLogger.log(Level.SEVERE, "JDBC SQLException in " + connectionDB
                        + ": " + e.getLocalizedMessage());
                hasException = true;
                exceptionMessage = e.getMessage();
            }
        }

        if (hasException)
            throw new FedException(new Throwable(exceptionMessage));

        // CREATE query is neither INSERT nor UPDATE so it will always return 0
        return 0;
    }

    private static int createTableHorizontal(String query) throws FedException {
        Statement statementOfDB1 = statementsMap.get(1);
        Statement statementOfDB2 = statementsMap.get(2);
        Statement statementOfDB3 = statementsMap.get(3);

    /*
     * Sets true if the Create query has to be deployed on first 2 DBs, that
     * means the list_of_boundaries for Horizontal Partitioning has only 1
     * boundary
     */
        boolean createFewerPartitionsThanDBs = createFewerPartitionsThanDBs(query);

        String queryForDB1 = buildPartitionedQueryForDB1(query,
                createFewerPartitionsThanDBs);
        String queryForDB2 = buildPartitionedQueryForDB2(query,
                createFewerPartitionsThanDBs);
        String queryForDB3 = buildPartitionedQueryForDB3(query,
                createFewerPartitionsThanDBs);

    /*
     * Taking advantage to form query from DB3 to DB2 when there is only one
     * boundary provided in list_of_boundaries for Horizontal Partitioning .
     */
        if (createFewerPartitionsThanDBs) {
            queryForDB2 = queryForDB3;
        }
        String fdbsCreated = "Query created by FDBS layer: ";
        try {
            CustomLogger.log(Level.INFO, fdbsCreated + queryForDB1
                    .replaceAll("  ", " ").replaceAll("\r\n", " ").replaceAll("\t", " "));
            CustomLogger.log(Level.INFO,
                    "Sending to:" + ConnectionConstants.CONNECTION_1_SID + ": "
                            + queryForDB1.replaceAll("  ", " ").replaceAll("\r\n", " ")
                            .replaceAll("\t", " "));
            statementOfDB1.executeUpdate(queryForDB1);
        } catch (SQLException e) {
            CustomLogger.log(Level.SEVERE,
                    "JDBC SQLException in " + ConnectionConstants.CONNECTION_1_SID + ": "
                            + e.getLocalizedMessage());
        }
        try {
            CustomLogger.log(Level.INFO, fdbsCreated + queryForDB2
                    .replaceAll("  ", " ").replaceAll("\r\n", " ").replaceAll("\t", " "));
            CustomLogger.log(Level.INFO,
                    "Sending to: " + ConnectionConstants.CONNECTION_2_SID + ": "
                            + queryForDB2.replaceAll("  ", " ").replaceAll("\r\n", " ")
                            .replaceAll("\t", " "));
            statementOfDB2.executeUpdate(queryForDB2);
        } catch (SQLException e) {
            CustomLogger.log(Level.SEVERE,
                    "JDBC SQLException in " + ConnectionConstants.CONNECTION_2_SID + ": "
                            + e.getLocalizedMessage());
        }
        try {
            if (!createFewerPartitionsThanDBs) {
                CustomLogger.log(Level.INFO,
                        fdbsCreated + queryForDB3.replaceAll("  ", " ")
                                .replaceAll("\r\n", " ").replaceAll("\t", " "));
                CustomLogger.log(Level.INFO,
                        "Sending to: " + ConnectionConstants.CONNECTION_3_SID + ": "
                                + queryForDB3.replaceAll("  ", " ").replaceAll("\r\n", " ")
                                .replaceAll("\t", " "));
                statementOfDB3.executeUpdate(queryForDB3);
            }
        } catch (SQLException e) {
            CustomLogger.log(Level.SEVERE,
                    "Failed to send to " + ConnectionConstants.CONNECTION_3_SID + ": "
                            + e.getLocalizedMessage());
        }

        // CREATE query is neither INSERT nor UPDATE so it will always return 0
        return 0;
    }

    /**
     * This method checks whether the list_of_boundaries provided in Horizontal
     * Partitioning has only 1 boundary. Returns true for 1 element.
     *
     * @param query
     */
    private static boolean createFewerPartitionsThanDBs(String query) {
        boolean createFewerPartitionsThanDBs = false;
        String columnName = query.substring(
                query.indexOf("HORIZONTAL (") + "HORIZONTAL (".length(),
                query.lastIndexOf("("));
        // Fetching range
        int firstIndex = query.lastIndexOf(columnName + "(")
                + (columnName.length() + 1);
        int secondIndex = query.lastIndexOf(",");

    /*
     * "True" means only 1 boundary provided for horizontal partitioning, so
     * table should be created in first 2 DBs.
     */
        if (secondIndex < firstIndex) {
            secondIndex = query.lastIndexOf("))");
            createFewerPartitionsThanDBs = true;
        }
        return createFewerPartitionsThanDBs;
    }

    private static String buildPartitionedQueryForDB1(String query,
                                                      boolean createFewerPartitionsThanDBs) {
        StringBuffer executableQuery = new StringBuffer();
        StringBuffer basicQuery = new StringBuffer(
                query.substring(0, query.indexOf("HORIZONTAL")));

        // Removes last ')' to further append constraint
        basicQuery = new StringBuffer(
                basicQuery.substring(0, basicQuery.lastIndexOf(")")));
        basicQuery.append(", constraint ");

        // Get values from the Query to build a constraint
        String tableName = query.substring("CREATE TABLE ".length(),
                query.indexOf(" ", "CREATE TABLE ".length()));
        String columnName = query.substring(
                query.indexOf("HORIZONTAL (") + "HORIZONTAL (".length(),
                query.lastIndexOf("("));

        // Fetching range
        int firstIndex = query.lastIndexOf(columnName + "(")
                + (columnName.length() + 1);
        int secondIndex = query.lastIndexOf(",");

        // If true it means it has one list of attribute for horizontal
        // partitioning
        if (createFewerPartitionsThanDBs) {
            secondIndex = query.lastIndexOf("))");
        }

        String maxRange = query.substring(firstIndex, secondIndex);

        // Appends constraint name
        basicQuery.append(tableName + "_" + columnName + "_HORIZ check (");
        basicQuery.append(columnName + " " + (maxRange.contains("\'") ? "<" : "<=")
                + " " + maxRange);
        basicQuery.append(")");

        // Adds back ')' after constraint is appended
        executableQuery = basicQuery.append(")");

        return executableQuery.toString();
    }

    private static String buildPartitionedQueryForDB2(String query,
                                                      boolean createFewerPartitionsThanDBs) {
        if (createFewerPartitionsThanDBs) {
            return "";
        }

        StringBuffer executableQuery = new StringBuffer();
        StringBuffer basicQuery = new StringBuffer(
                query.substring(0, query.indexOf("HORIZONTAL")));

        // Removes last ')' to further append constraint
        basicQuery = new StringBuffer(
                basicQuery.substring(0, basicQuery.lastIndexOf(")")));

        basicQuery.append(", constraint ");

        // Get values from Query to build constraint
        String tableName = query.substring("CREATE TABLE ".length(),
                query.indexOf(" ", "CREATE TABLE ".length()));
        String columnName = query.substring(
                query.indexOf("HORIZONTAL (") + "HORIZONTAL (".length(),
                query.lastIndexOf("("));
        String lowerRange = query.substring(
                query.indexOf(columnName + "(") + (columnName.length() + 1),
                query.lastIndexOf(","));
        String upperRange = query.substring(
                query.indexOf(lowerRange + ",") + (lowerRange + ",").length(),
                query.lastIndexOf("))"));

        // Appends constraint name
        basicQuery.append(tableName + "_" + columnName + "_HORIZ check (");
        basicQuery.append(columnName);
        basicQuery
                .append(
                        " between "
                                + (lowerRange.contains("\'") ? lowerRange
                                : (Integer.parseInt(lowerRange) + 1))
                                + " and " + upperRange);
        basicQuery.append(")");

        // Adds back ')' after constraint is appended
        executableQuery = basicQuery.append(")");

        return executableQuery.toString();
    }

    private static String buildPartitionedQueryForDB3(String query,
                                                      boolean createFewerPartitionsThanDBs) {
        StringBuffer executableQuery = new StringBuffer();
        StringBuffer basicQuery = new StringBuffer(
                query.substring(0, query.indexOf("HORIZONTAL")));

        String operator = "";
        if (createFewerPartitionsThanDBs) {
            operator = " >= ";
        } else {
            operator = " > ";
        }

        // Removes last ')' to further append constraint
        basicQuery = new StringBuffer(
                basicQuery.substring(0, basicQuery.lastIndexOf(")")));

        basicQuery.append(", constraint ");

        // Get values from Query to build constraint
        String tableName = query.substring("CREATE TABLE ".length(),
                query.indexOf(" ", "CREATE TABLE ".length()));
        String columnName = query.substring(
                query.indexOf("HORIZONTAL (") + "HORIZONTAL (".length(),
                query.lastIndexOf("("));

        String maxRange = "";
        if (createFewerPartitionsThanDBs) {
            maxRange = query.substring(
                    query.indexOf(columnName + "(") + (columnName.length() + 1),
                    query.lastIndexOf("))"));
        } else {
            maxRange = query.substring(query.lastIndexOf(",") + 1,
                    query.lastIndexOf("))"));
        }

        // Appends constraint name
        basicQuery.append(tableName + "_" + columnName + "_HORIZ check (");
        basicQuery.append(columnName + operator + maxRange);
        basicQuery.append(")");

        // Adds back ')' after constraint is appended
        executableQuery = basicQuery.append(")");

        return executableQuery.toString();
    }

    private static int dropTable(String query) throws FedException {
        int result = -1;
        String connectionDB = "";
        Integer connectionNumber = -1;

        Statement statement = null;

        String exMessage = null;        //store exception message to use later
        //track where exceptions occur
        List<String> exOrigin = new ArrayList<String>();

        for (Integer statementKey : statementsMap.keySet()) {
            connectionNumber = statementKey;
            if (statementKey == 1) {
                connectionDB = ConnectionConstants.CONNECTION_1_SID;
            }
            if (statementKey == 2) {
                connectionDB = ConnectionConstants.CONNECTION_2_SID;
            }
            if (statementKey == 3) {
                connectionDB = ConnectionConstants.CONNECTION_3_SID;
            }

            statement = statementsMap.get(statementKey);
            CustomLogger.log(Level.INFO, "Sending to " + connectionDB + ": " + query
                    .replaceAll("  ", " ").replaceAll("\r\n", " ").replaceAll("\t", " "));
            try {
                result = statement.executeUpdate(query);
            } catch (SQLException e) {
                exMessage = e.getMessage();
                CustomLogger.log(Level.SEVERE,
                        "SQLException in " + connectionDB + ":" + exMessage);
                exOrigin.add(connectionDB);
            }
            //continue;
        }
        if (exOrigin.size() > 0) {
            throw new FedException(new Throwable("SQLException occured in "
                    + String.join(",", exOrigin) + ": " + exMessage));
        }
        return result;
    }

    /*
     * Not used yet but might be used in further implementation. Parser requires
     * query as InputStream, so this method converts String queries and returns
     * List of parse-able InputStream queries
     */
    public static List<InputStream> convertToParsableQueries(
            List<String> queries) {
        List<InputStream> parsableQueries = new ArrayList<InputStream>();
        for (int i = 0; i < queries.size(); i++) {
            parsableQueries.add(convertToParsableQuery(queries.get(i)));
        }
        return parsableQueries;
    }

    /*
     * Parser requires query as InputStream, so this method converts String query
     * and returns parse-able InputStream query
     */
    public static InputStream convertToParsableQuery(String query) {
        return new ByteArrayInputStream(query.getBytes());
    }

    public static void setFedStatement(FedStatement statement) {
        fedStatement = statement;
    }

    public static FedResultSet executeSelectQuery(String query)
            throws FedException {
        // Do not execute HAVING query
        if (query.toUpperCase().contains(" HAVING ")) {
            throw new FedException(new Throwable("\'HAVING\' is not supported."));
        }

        FedResultSet instance = null;
        int queryType = QueryTypeConstant.NONE;

    /* Some complex preprocess start */

    /*
     * Removes tabs, extra spaces and lines for fdbs.parser to understand according
     * to the grammar. NOTE: We use this method because skipping tabs, spaces
     * and new lines does not work efficiently.
     */
        query = processQueryForParser(query);

    /* Some complex preprocess end */

        // Every query needs ';' to parse, so being added here.
        // Parsing starts here.
        GepardParser parser = new GepardParser(convertToParsableQuery(query + ";"));

        // This method is a general method from where all grammar starts.
        try {
            queryType = parser.ParseQuery();
        } catch (ParseException e) {
            throw new FedException(new Throwable(e.getMessage()));
        }

    /* Some complex post process start before going to database */

    /*
     * Special characters like umlauts were replaced with unicode equivalents
     * while running processQueryForParser method. Reason: JavaCC does not
     * support umlauts
     */
        query = UnicodeManager.replaceUnicodesWithChars(query);
        query = replace3DashesWithSpace(query);
        query = replaceBraces(query);

    /* Some complex post process end before going to database */

        switch (queryType) {
            case QueryTypeConstant.SELECT_COUNT_ALL_TABLE:
                instance = selectCountAllTable(query);
                break;
            //            case QueryTypeConstant.SELECT_WITH_GROUP:
            //                System.out.println("with group");
            //                break;
            //            case QueryTypeConstant.SELECT_WITHOUT_GROUP:
            //                System.out.println("without group");
            //                break;
            default:
                instance = executeQuery(query);
        }

        return instance;
    }

    private static FedResultSet executeQuery(String query) throws FedException {
        List<ResultSet> resultSets = new ArrayList<>();

        for (Statement statement : statementsMap.values()) {
            try {
                resultSets.add(statement.executeQuery(query));
            } catch (SQLException e) {
                throw new FedException(e.getCause());
            }
        }

        return new FedResultSet(resultSets);

    }

    private static FedResultSet selectCountAllTable(String query)
            throws FedException {
        List<ResultSet> resultSets = new ArrayList<>();

        for (Statement statement : statementsMap.values()) {
            try {
                resultSets.add(statement.executeQuery(query));
            } catch (SQLException e) {
                throw new FedException(e.getCause());
            }
        }

        ResultSet rs = new SelectCountResultSet(resultSets);
        resultSets = new ArrayList<>();
        resultSets.add(rs);
        return new FedResultSet(resultSets);

    }
}
