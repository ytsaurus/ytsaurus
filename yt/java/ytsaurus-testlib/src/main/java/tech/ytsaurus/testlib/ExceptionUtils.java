package tech.ytsaurus.testlib;

import tech.ytsaurus.lang.NonNullApi;

@NonNullApi
public class ExceptionUtils {
    private ExceptionUtils() {
    }

    public static <T extends Throwable> T getCause(Throwable ex, Class<T> causeClass) {
        for (Throwable cur = ex; cur != null; cur = cur.getCause()) {
            if (causeClass.isInstance(cur)) {
                //noinspection unchecked
                return (T) cur;
            }
        }
        throw new RuntimeException("Exception " + ex + " is not caused by " + causeClass);
    }
}
