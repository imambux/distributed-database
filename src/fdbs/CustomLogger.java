/**
 *
 */
package fdbs;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Logger Wrapper Class
 * The logger will be used by several part of the application to log several info.
 * It sets the configuration. For example, file name, formatting of the texts etc.
 *
 * @author Jahan
 */
public class CustomLogger {

    static Logger logger;
    public FileHandler fh;

    /**
     * @param null
     */
    private CustomLogger() throws IOException {
        //instance the logger
        logger = Logger.getLogger(CustomLogger.class.getName());
        //FileHandler fh;
        System.setProperty("java.util.logging.SimpleFormatter.format", "<%1$tT.%1$tL>%5$s %n");
        //instance the filehandler
        fh = new FileHandler("logs/fedprot.txt");

        //instance formatter, set formatting, and handler
        SimpleFormatter formatter = new SimpleFormatter();
        fh.setFormatter(formatter);
        logger.addHandler(fh);

        // disable output to console
        logger.setUseParentHandlers(false);

    }

    /**
     * @param null
     */
    private static Logger getLogger() {
        if (logger == null) {
            try {
                new CustomLogger();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return logger;
    }

    /**
     * @param level msg
     */
    public static void log(Level level, String msg) {
        getLogger().log(level, msg);

    }

}
