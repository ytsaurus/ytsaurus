package tech.ytsaurus.core.utils;

public class ExceptionUtils {
    private ExceptionUtils() {
    }

    public static boolean hasCause(Throwable chain, Class<? extends Throwable> type) {
        if (type.isInstance(chain)) {
            return true;
        }
        boolean result = false;
        if (chain.getCause() != null) {
            result = hasCause(chain.getCause(), type);
        }
        if (!result && chain.getSuppressed() != null) {
            for (Throwable ex : chain.getSuppressed()) {
                result = hasCause(ex, type);
                if (result) {
                    return result;
                }
            }
        }
        return result;
    }
}
