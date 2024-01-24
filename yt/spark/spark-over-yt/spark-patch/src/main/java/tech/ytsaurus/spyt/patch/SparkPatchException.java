package tech.ytsaurus.spyt.patch;

import java.io.IOException;

public class SparkPatchException extends RuntimeException {

    public SparkPatchException(Exception cause) {
        super(cause);
    }

    public SparkPatchException(String message) {
        super(message);
    }
}
