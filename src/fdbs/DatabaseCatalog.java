package fdbs;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class DatabaseCatalog {

    private static HashMap<Integer, Statement> statementsMap;
    private static HashMap<Integer, HashMap<String, String>> partitionedTables = new HashMap<>();
    private static HashMap<Integer, Set<String>> allTablesWithDBs = new HashMap<>();


    public static void setStatementsMap(HashMap<Integer, Statement> map) {
        statementsMap = map;
        initCatalog();
    }

    private static void initCatalog() {
        // Sets partitioned tables
        Statement statement = null;
        for (Integer statementKey : statementsMap.keySet()) {
            statement = statementsMap.get(statementKey);
            try {
                ResultSet rs = statement.executeQuery("SELECT TABLE_NAME, SEARCH_CONDITION, CONSTRAINT_NAME " + "FROM USER_CONSTRAINTS");
                Set<String> tables = new HashSet<>();
                HashMap<String, String> tablesPartitioned = new HashMap();
                while (rs.next()) {
                    String tableName = rs.getString(1);
                    String searchCondition = rs.getString(2);
                    String constraintName = rs.getString(3);

                    tables.add(tableName);
                    if (constraintName != null && constraintName.toUpperCase().contains("HORIZ")) {
                        tablesPartitioned.put(searchCondition, tableName);
                    }
                }
                allTablesWithDBs.put(statementKey, tables);
                partitionedTables.put(statementKey, tablesPartitioned);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    public static boolean isPartitioned(String table) {
        boolean isPartitioned = false;
        Set<Integer> dbs = partitionedTables.keySet();
        for (Integer dbNumber : dbs) {
            HashMap<String, String> constraintConditionsWithTableNames = partitionedTables.get(dbNumber);
            for (String str : constraintConditionsWithTableNames.values()) {
                if (table.trim().toUpperCase().equals(str.trim().toUpperCase())) {
                    return true;
                }
            }
        }
        return isPartitioned;
    }

    public static HashMap<Integer, Statement> getStatementsForQuery(String query) {
        HashMap<Integer, Statement> statements = new HashMap<>();
        if (isPartitionedTable(query)) {
            String table = getTableFromInsertQuery(query);
            for (Integer dbNumber : partitionedTables.keySet()) {
                HashMap<String, String> constraintConditionsWithTableNames = partitionedTables.get(dbNumber);
                for (String str : constraintConditionsWithTableNames.values()) {
                    if (table.trim().toUpperCase().equals(str.trim().toUpperCase())) {
//                        Set<String> constraints = constraintConditionsWithTableNames.keySet();
//                        for (String constraint : constraints) {
//                            String temp = constraint.substring(constraint.lastIndexOf(" "));
//                            if(temp.contains("\'")) {
////                                constraint.replace()
//                            } else {
//
//                            }
//                        }
                        statements.put(dbNumber, statementsMap.get(dbNumber));
                        break;
                    }
                }
            }
        }
        return statements;
    }

    public static boolean isPartitionedTable(String query) {
        if (!query.trim().toUpperCase().startsWith("INSERT")) {
            return false;
        }

        String table = DatabaseCatalog.getTableFromInsertQuery(query);
        return DatabaseCatalog.isPartitioned(table);
    }

    public static String getTableFromInsertQuery(String query) {
        query = query.trim().toUpperCase();
        return query.substring(query.indexOf("INTO ") + 5, query.indexOf(" VALUE")).trim();
    }

    public static void disableAllReferentialConstraints(String table, Statement statement) {
        try {
            ResultSet constraintsSet = statement.executeQuery("SELECT CONSTRAINT_NAME FROM USER_CONSTRAINTS WHERE TABLE_NAME = '" + table + "' AND CONSTRAINT_TYPE = 'R'");
            StringBuilder query = new StringBuilder("alter table " + table);
            while (constraintsSet.next()) {
                query.append(" disable constraint " + constraintsSet.getString(1));
            }
            statement.execute(query.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}

