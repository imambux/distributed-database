package application;

import fjdbc.*;
import parser.ParseException;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/*
 * This is also the application to use federated implemention of database, same as App.java in hsfulda.de package
 */
public class Application {

    // Variables to keep track of time
    private static long startTime;
    private static long duration;

    public static void main(String[] args) throws ParseException, FedException {

        // Program does not terminate until user clicks "Cancel" button in a prompted dialogue.
        while (true) {

            // Selects files and stores statements in a list
            File selectedFile = FileUtility.getFile();
            List<String> statementsFromFile = FileUtility.getStatementsFromFile(selectedFile);

            // All statements taken from file provided run here one by one
            int totalOperations = 0;
            int successfulOperations = 0;
            int failedOperations = 0;

            FedConnection fedConnection = null;
            try {
                // Prepares resources
                fedConnection = new FedPseudoDriver().getConnection(ApplicationConstants.USERNAME, ApplicationConstants.PASSWORD);
                fedConnection.setAutoCommit(false);
                FedStatement fedStatement = fedConnection.getStatement();

                // Prints file name
                OutputFormatter.printFileName(selectedFile);
                System.out.println("\nFile: \'" + selectedFile.getName() + "\'\n");

                // Recording time starts
                startTime = System.currentTimeMillis();

                for (String currentStatement : statementsFromFile) {
                    // Ignores "SET" or "ALTER" script
                    if (shouldNotParse(currentStatement)) {
                        continue;
                    }

                    try {
                        // DDL and DML
                        if (isDDLOrDMLScript(currentStatement)) {
                            fedStatement.executeUpdate(currentStatement);
                            System.out.print("\nSUCCESSFUL - ");
                            OutputFormatter.printQuery(currentStatement);
                        }
                        // Commit
                        else if (isCommitScript(currentStatement)) {
                            fedConnection.commit();
                            System.out.print("\nSUCCESSFUL - ");
                            OutputFormatter.printQuery(currentStatement);
                        }
                        // Rollback
                        else if (isRollbackScript(currentStatement)) {
                            fedConnection.rollback();
                            System.out.print("\nSUCCESSFUL - ");
                            OutputFormatter.printQuery(currentStatement);
                        }
                        // Select
                        else {
                            FedResultSet resultSet = fedStatement.executeQuery(currentStatement);
                            System.out.print("\nSUCCESSFUL - ");
                            OutputFormatter.printQuery(currentStatement);
                            System.out.println();
                            printResult(resultSet);
                        }

                        successfulOperations++;
                    } catch (FedException e) {
                        failedOperations++;
                        System.out.print("\nFAILED - ");
                        OutputFormatter.printQuery(currentStatement);
                        System.out.println("\nMESSAGE - " + e);
                    }
                    totalOperations++;
                }
            } catch (Exception e) {
                System.out.println("Exception; " + e);
            }
            // @author: Anfilov. Close all JDBC connections
            finally {
                fedConnection.close();
            }

            // Prints time taken
            System.out.println("\n\nTotal Operations: " + totalOperations + ", Successful: " + successfulOperations
                    + ", Failed: " + failedOperations);
            System.out.println(getTimeTaken());
            OutputFormatter.printAsterisks();
            System.out.println();

            // Prompts dialog box for user to either load another script file or to terminate the program
            String message = "File \"" + selectedFile.getName() + "\" execution completed. Do you want to load another SQL script?";
            if (FileUtility.showConfirmDialog(message)) {
                continue;
            }

            break;
        }

        System.out.println("PROGRAM ENDS");
    }

    private static boolean shouldNotParse(String query) {
        query = query.trim().toUpperCase();
        return query.startsWith("SET") || query.startsWith("ALTER");
    }

    private static void printResult(FedResultSet resultSet) throws FedException {

        if (resultSet == null) {
            System.out.println("ResultSet is \'NULL\'");
            return;
        }

        List<String> columnNames = new ArrayList<String>();
        List<String> columnTypes = new ArrayList<String>();
        List<String> records = new ArrayList<String>();

        int numberOfColumns = resultSet.getColumnCount();

        int counter = 1;
        String column = null;
        while (counter <= numberOfColumns) {
            String columnName = resultSet.getColumnName(counter);
            columnNames.add(String.format("%.12s", String.format("%-12s", columnName)));
            column = resultSet.getColumnType(counter);
            columnTypes.add(column);

            counter++;
        }

        while (resultSet.next()) {
            StringBuilder record = new StringBuilder();
            counter = 0;
            while (counter < numberOfColumns) {
                String columnType = columnTypes.get(counter);
                String columnValue = "";
                if (columnType.equals("INTEGER") || columnType.equals("NUMBER"))
                    columnValue = resultSet.getInt(counter + 1) + "";
                else if (columnType.equals("VARCHAR")) columnValue = resultSet.getString(counter + 1);
                record.append(String.format("%.12s", String.format("%-12s", columnValue)));
                counter++;
            }
            // @author: Jahan. Check for duplication. Only insert unique data
            if (!records.contains(record.toString())) {
                records.add(record.toString());
            } else {
                continue;
            }
        }

        String columnNamesStr = "";
        for (String string : columnNames) {
            columnNamesStr += string;
        }

        counter = 1;
        while (counter <= numberOfColumns) {
            OutputFormatter.printDashes(12);
            counter++;
        }
        System.out.print("\n");
        System.out.println(columnNamesStr);
        counter = 1;
        while (counter <= numberOfColumns) {
            OutputFormatter.printDashes(12);
            counter++;
        }
        System.out.print("\n");
        for (String string : records) {
            System.out.println(string);
        }
    }

    private static String getColumnTypeStr(int columnTypeInt) {
        String columnType = "";
        switch (columnTypeInt) {
            case 1:
                columnType = "INTEGER";
                break;
            case 2:
                columnType = "VARCHAR";
                break;
            default:
        }
        return columnType;
    }

    private static boolean isRollbackScript(String script) {
        return script.trim().toUpperCase().startsWith("ROLLBACK");
    }

    private static boolean isCommitScript(String script) {
        return script.trim().toUpperCase().startsWith("COMMIT");
    }

    private static boolean isDDLOrDMLScript(String script) {
        script = script.trim().toUpperCase();
        return script.startsWith("CREATE") || script.startsWith("DROP") || script.startsWith("INSERT") || script.startsWith("DELETE") || script.startsWith("UPDATE");
    }

    private static String getTimeTaken() {
        duration = System.currentTimeMillis() - startTime;
        Date timeTaken = new Date(duration);
        String timeTakenStr = String.format(
                "Time Taken : %2d Min : %2d Sec : %3d Millis", timeTaken.getMinutes(),
                timeTaken.getSeconds(), (timeTaken.getTime() % 1000));
        return timeTakenStr;
    }


}
