package tech.ytsaurus.client.operations;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;

public class YtUtils {
    private static final boolean IS_WINDOWS = System.getProperty("os.name").startsWith("Windows");

    private YtUtils() {
    }

    private static String pseudoFileNameById(int number) {
        return String.format("/dev/fd/%d", number);
    }

    public static OutputStream outputStreamById(int number) throws FileNotFoundException {
        if (IS_WINDOWS) {
            throw new RuntimeException("Unsupported OS");
        } else {
            return new FileOutputStream(pseudoFileNameById(number));
        }
    }
}
