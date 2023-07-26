package tech.ytsaurus.testlib;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.LogManager;

public class LoggingUtils {
    private LoggingUtils() {
    }

    /**
     * Reads and initializes the logging configuration from the given input stream
     *
     * @param ins stream to read properties from
     */
    public static void loadJULConfig(InputStream ins) {
        try {
            LogManager.getLogManager().readConfiguration(ins);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
