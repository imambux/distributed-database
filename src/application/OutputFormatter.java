package application;

import java.io.File;

public class OutputFormatter {

    public static void printAsterisks() {
        int i = 75;
        while (i > 0) {
            System.out.print('*');
            i--;
        }
        System.out.println();
    }

    public static void printDashes() {
        int i = 14;
        while (i > 0) {
            System.out.print('-');
            i--;
        }
        System.out.println();
    }

    public static void printDashes(int quantity) {
        int i = quantity;
        while (i > 0) {
            System.out.print('-');
            i--;
        }
    }

    public static void printFileName(File file) {
        OutputFormatter.printAsterisks();
        System.out.println("Executing script file \'" + file.getAbsolutePath() + "\' ...");
        OutputFormatter.printAsterisks();
    }

    public static void printQuery(String str) {
        System.out.print(str.length() > 59 ? str.substring(0, 59) + "..." : str + ";");
    }
}
