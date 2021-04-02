package ru.yandex.yt.ytclient.proxy;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.rpc.RpcError;
import ru.yandex.yt.ytclient.rpc.RpcErrorCode;
import ru.yandex.yt.ytclient.rpc.RpcOptions;

/**
 * Class determines which errors must be retried.
 *
 * <p>
 *     Users must not override this class, instead they should use factory methods.
 * </p>
 * <p>
 *     Example below creates retry policy that retries codes 100 and 500 no more than 10 times:
 *     <code>
 *         RequestRetryPolicy.attemptLimited(10, RequestRetryPolicy.forCodes(100, 500))
 *     </code>
 * </p>
 */
@NonNullApi
public abstract class RetryPolicy {
    private RetryPolicy() {
    }

    /**
     * Create retry policy that disallows all retries.
     */
    public static RetryPolicy noRetries() {
        return new NoRetryPolicy();
    }

    /**
     * Create retry policy that will retry given set of error codes.
     */
    public static RetryPolicy forCodes(Collection<Integer> errorCodes) {
        return new YtErrorRetryPolicy(errorCodes);
    }

    /**
     * Create retry policy that will retry given set of error codes.
     */
    public static RetryPolicy forCodes(Integer... errorCodes) {
        return forCodes(Arrays.asList(errorCodes));
    }

    /**
     * Wrap other retry policy to limit total number of attempts.
     */
    public static RetryPolicy attemptLimited(int attemptLimit, RetryPolicy inner) {
        return new AttemptLimitedRetryPolicy(attemptLimit, inner);
    }

    abstract Optional<Duration> getBackoffDuration(Throwable error, RpcOptions options);

    void onNewAttempt() {
    }

    String getTotalRetryCountDescription() {
        return "<unknown>";
    }

    @NonNullApi
    static class YtErrorRetryPolicy extends RetryPolicy {
        private final Set<Integer> errorCodesToRetry;
        private final BackoffProvider backoffProvider = new BackoffProvider();

        YtErrorRetryPolicy(Collection<Integer> codesToRetry) {
            this.errorCodesToRetry = new HashSet<>(codesToRetry);
        }

        @Override
        Optional<Duration> getBackoffDuration(Throwable error, RpcOptions options) {
            TimeoutException timeoutException = null;
            IOException ioException = null;
            RpcError rpcError = null;
            while (error != null) {
                if (error instanceof TimeoutException) {
                    timeoutException = (TimeoutException) error;
                } else if (error instanceof IOException) {
                    ioException = (IOException) error;
                } else if (error instanceof RpcError) {
                    rpcError = (RpcError) error;
                }
                error = error.getCause();
            }

            if (rpcError != null) {
                if (rpcError.matches(errorCodesToRetry::contains)) {
                    return Optional.of(backoffProvider.getBackoffTime(rpcError, options));
                } else {
                    return Optional.empty();
                }
            }
            if (timeoutException != null) {
                return Optional.of(Duration.ZERO);
            }
            if (ioException != null) {
                return Optional.of(Duration.ZERO);
            }
            return Optional.empty();
        }
    }

    @NonNullApi
    static class AttemptLimitedRetryPolicy extends RetryPolicy {
        private final int attemptLimit;
        private final RetryPolicy inner;
        private int currentAttempt = 0;

        AttemptLimitedRetryPolicy(int attemptLimit, RetryPolicy inner) {
            this.attemptLimit = attemptLimit;
            this.inner = inner;
        }

        @Override
        public Optional<Duration> getBackoffDuration(Throwable error, RpcOptions options) {
            if (currentAttempt < attemptLimit) {
                return inner.getBackoffDuration(error, options);
            } else {
                return Optional.empty();
            }
        }

        @Override
        public void onNewAttempt() {
            currentAttempt += 1;
            inner.onNewAttempt();
        }

        @Override
        public String getTotalRetryCountDescription() {
            return Integer.toString(attemptLimit);
        }
    }

    static class NoRetryPolicy extends RetryPolicy {
        @Override
        Optional<Duration> getBackoffDuration(Throwable error, RpcOptions options) {
            return Optional.empty();
        }
    }
}

@NonNullApi
@NonNullFields
class BackoffProvider {
    private @Nullable Duration currentExponentialBackoff = null;

    Duration getBackoffTime(RpcError error, RpcOptions options) {
        Set<Integer> errorCodes = error.getErrorCodes();
        if (errorCodes.stream().anyMatch(BackoffProvider::isExponentialBackoffError)) {
            if (currentExponentialBackoff == null) {
                currentExponentialBackoff = options.getMinBackoffTime();
                return currentExponentialBackoff;
            }

            currentExponentialBackoff = Collections.max(Arrays.asList(
                    currentExponentialBackoff.plus(Duration.ofSeconds(1)),
                    currentExponentialBackoff.multipliedBy(2)
            ));
            assert currentExponentialBackoff != null;

            Duration maxBackoffTime = options.getMaxBackoffTime();
            if (currentExponentialBackoff.compareTo(maxBackoffTime) > 0) {
                currentExponentialBackoff = options.getMaxBackoffTime();
            }
            return currentExponentialBackoff;
        }
        return options.getMinBackoffTime();
    }

    private static boolean isExponentialBackoffError(int errorCode) {
        return errorCode == RpcErrorCode.RequestQueueSizeLimitExceeded.code ||
                errorCode == 904; // NSecurityClient.RequestQueueSizeLimitExceeded
    }
}
