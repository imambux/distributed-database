package fdbs;

import application.ApplicationConstants;
import parser.ParseException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Helper {

    public static InputStream convertToParsableQuery(String query) {
        InputStream parsableQuery = new ByteArrayInputStream(query.getBytes());
        return parsableQuery;
    }

    public static void PrintResult(String query) throws SQLException {
        Connection conn = null;
        Statement stmt = null;
        conn = DriverManager.getConnection(ConnectionConstants.CONNECTION_3_URL, ApplicationConstants.USERNAME, ApplicationConstants.PASSWORD);
        query = "SELECT * FROM PASSAGIER";

        try {
            DatabaseMetaData md = conn.getMetaData();
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            ResultSetMetaData rsmd = rs.getMetaData();

            int columnCount = rsmd.getColumnCount();

            //Get Catalog
            ResultSet catalogs = md.getCatalogs();

            while (catalogs.next()) {
                System.out.println("catalog: ");
                String catalog = catalogs.getString(1);  //"TABLE_CATALOG"
                System.out.println("catalog: " + catalog);
            }
            // Initialize the metadata list
            List<String> x = new ArrayList<String>();

            for (int i = 1; i <= columnCount; i++) {
                String name = rsmd.getColumnName(i);
                // Push to array
                x.add(name);
                // Print Columns
                System.out.printf("%-10s", name);
            }
            System.out.print("\n----------");
            System.out.print("-------------------------------");
            System.out.print("\n");
            while (rs.next()) {
                for (int i = 0; i < columnCount; i++) {
                    //Display values
                    System.out.printf("%-10s", rs.getString(x.get(i)));
                }
                System.out.print("\n");
            }
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    //TODO delete the main method
    public static void main(String[] args) throws ParseException {
        try {
            // Pass query
            PrintResult("s");
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    /**
     * @parameter query
     * @author Jahan
     */
    public static List<String> GetColumnNames(String dbname, String table) throws SQLException {
        List<String> x = new ArrayList<String>();

        Connection conn = null;
        Statement stmt = null;
        conn = DriverManager.getConnection(ConnectionConstants.CONNECTION_3_URL, ApplicationConstants.USERNAME, ApplicationConstants.PASSWORD);
        String query = "SELECT * FROM " + table;

        try {
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            ResultSetMetaData rsmd = rs.getMetaData();

            int columnCount = rsmd.getColumnCount();
            // Initialize the metadata list
            for (int i = 1; i <= columnCount; i++) {
                String name = rsmd.getColumnName(i);
                // Push to array
                x.add(name);
                // Print Columns

            }
        } catch (SQLException e) {
            System.out.println(e);
        }
        return x;
    }

}
