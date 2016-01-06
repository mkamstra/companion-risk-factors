package no.stcorp.com.companion.logging;

import java.io.IOException;
import java.util.logging.*;

public class CompanionLogger {
  private static FileHandler fileTxt;
  private static SimpleFormatter formatterTxt;
  private static FileHandler fileHTML;
  private static Formatter formatterHTML;

  public static void setup(String loggerName) throws IOException {
    // get the global logger to configure it
    //Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    Logger logger = Logger.getLogger(loggerName);

    // suppress the logging output to the console
    Logger rootLogger = Logger.getLogger("");
    Handler[] handlers = rootLogger.getHandlers();
    if (handlers[0] instanceof ConsoleHandler) {
      rootLogger.removeHandler(handlers[0]);
    }

    logger.setLevel(Level.INFO);
    fileTxt = new FileHandler("Logging" + loggerName + ".txt");
    fileHTML = new FileHandler("Logging" + loggerName + ".html");

    // create a TXT formatter
    formatterTxt = new SimpleFormatter();
    fileTxt.setFormatter(formatterTxt);
    logger.addHandler(fileTxt);

    // create an HTML formatter
    formatterHTML = new LoggerHtmlFormatter();
    fileHTML.setFormatter(formatterHTML);
    logger.addHandler(fileHTML);
  }
}
 