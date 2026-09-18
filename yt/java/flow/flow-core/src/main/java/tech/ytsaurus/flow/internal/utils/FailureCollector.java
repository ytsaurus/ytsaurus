package tech.ytsaurus.flow.internal.utils;

import java.util.Objects;
import java.util.concurrent.Callable;

import org.jspecify.annotations.Nullable;

/**
 * Collects failures without skipping cleanup: JVM-fatal errors precede other Errors, then Exceptions.
 */
public final class FailureCollector {
    private @Nullable Throwable failure;

    public void add(Throwable next) {
        Objects.requireNonNull(next);
        if (failure == null) {
            failure = next;
        } else if (failure != next) {
            if (priority(next) > priority(failure)) {
                next.addSuppressed(failure);
                failure = next;
            } else {
                failure.addSuppressed(next);
            }
        }
    }

    private static int priority(Throwable error) {
        return error instanceof VirtualMachineError ? 2 : error instanceof Error ? 1 : 0;
    }

    public boolean tryRun(Runnable action) {
        try {
            action.run();
            return true;
        } catch (Throwable error) {
            add(error);
            return false;
        }
    }

    /**
     * Returns the collected exception, or rethrows an Error unchanged.
     */
    public Exception asException() {
        Throwable error = Objects.requireNonNull(failure, "No failure was collected");
        if (error instanceof Error fatal) {
            throw fatal;
        }
        if (error instanceof Exception exception) {
            return exception;
        }
        return new IllegalStateException("Unexpected failure", error);
    }

    public void throwUncheckedIfAny() {
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure instanceof RuntimeException exception) {
            throw exception;
        }
        if (failure != null) {
            throw new IllegalStateException("Unexpected checked cleanup failure", failure);
        }
    }

    /**
     * Runs cleanup exactly once, including when the body fails. Unlike try-with-resources, a fatal
     * cleanup error cannot be suppressed behind an ordinary body failure.
     */
    public static <T> T callWithCleanup(Callable<T> body, Runnable cleanup) throws Exception {
        T result;
        try {
            result = body.call();
        } catch (Throwable error) {
            var failures = new FailureCollector();
            failures.add(error);
            failures.tryRun(cleanup);
            throw failures.asException();
        }
        cleanup.run();
        return result;
    }
}
