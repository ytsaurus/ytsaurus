package tech.ytsaurus.yson;

public class YsonError extends RuntimeException {
    public YsonError() {
    }

    public YsonError(String message) {
        super(message);
    }

    public YsonError(String message, Throwable cause) {
        super(message, cause);
    }

    public YsonError(Throwable cause) {
        super(cause);
    }
}
