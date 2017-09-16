package application;

import javax.swing.*;
import java.awt.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class FileUtility {

    // Fetches file with GUI based on OS
    public static File getFile() {
        File sqlFile;

        final Frame f = new Frame();
        final FileDialog fd = new FileDialog(f, "Load SQL Batch file (.sql)", FileDialog.LOAD);
        fd.setAlwaysOnTop(true);
        fd.setVisible(true);
        final String path = fd.getDirectory();
        final String filename = fd.getFile();
        f.dispose();

        sqlFile = new File(path + filename);

        String message = "Only SQL Dump (.sql) file is accepted.\nDo you want to try loading SQL file again?";
        if (!(sqlFile != null && sqlFile.getAbsolutePath().toLowerCase().endsWith("sql"))) {
            if (showConfirmDialog(message)) {
                getFile();
            } else {
                System.exit(1);
            }
        }

        return sqlFile;
    }

    public static boolean showConfirmDialog(String message) {
        int selectedOption = JOptionPane.showConfirmDialog(null, message, "Choose", JOptionPane.YES_NO_OPTION, JOptionPane.INFORMATION_MESSAGE);
        if (selectedOption == JOptionPane.YES_OPTION) {
            return true;
        } else {
            return false;
        }
    }


    public static List<String> getStatementsFromFile(File file) {
        List<String> sqlStatements = new ArrayList<String>();

        BufferedReader br = null;
        try {
            StringBuilder script = new StringBuilder();
            String line;

            br = new BufferedReader(new FileReader(file));

            while ((line = br.readLine()) != null) {
                line = fetchValidLine(line);

                if (line.trim().isEmpty())
                    continue;

                if (line.contains(";")) {
                    String[] strArr = line.split(";");
                    for (String str : strArr) {
                        if (str.isEmpty())
                            continue;

                        script.append("  " + str);
                    }

                    if (script.length() > 0) {
                        script = new StringBuilder(refineQuery(script.toString()));
                        sqlStatements.add(script.toString().trim());
                    }
                    script = new StringBuilder();
                } else {
                    // @author: Jahan. Added a blank space to create a proper sql statement
                    script.append("  " + line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null)
                    br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        return sqlStatements;
    }

    private static String refineQuery(String script) {
        while (script.contains("\n")) {
            script = script.replaceAll("\n", " ");
        }
        while (script.contains("\t")) {
            script = script.replaceAll("\t", " ");
        }
        while (script.contains("  ")) {
            script = script.replaceAll("  ", " ");
        }

        return script;
    }

    // Does not consider comments (e.g. -- // /* \\ # as scripts
    private static String fetchValidLine(String script) {
        StringBuilder validLine = new StringBuilder();
        Scanner input = new Scanner(script.toString());

        while (input.hasNextLine()) {
            String line = input.nextLine();
            if (line.startsWith("--") || line.startsWith("\\\\") || line.startsWith("//") || line.startsWith("/*")
                    || line.startsWith("#") || line.trim().isEmpty())
                continue;
            else {
                validLine.append(line);
            }
        }
        return validLine.toString();
    }

}
