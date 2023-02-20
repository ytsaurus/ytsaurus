package tech.ytsaurus.client;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcError;
import tech.ytsaurus.client.rpc.RpcErrorCode;
import tech.ytsaurus.client.rpc.RpcFailoverPolicy;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

/**
 * Class determines which errors must be retried.
 *
 * <p>
 * Users must not override this class, instead they should use factory methods.
 * </p>
 * <p>
 * Example below creates retry policy that retries codes 100 and 500 no more than 10 times:
 * <code>
 * RequestRetryPolicy.attemptLimited(10, RequestRetryPolicy.forCodes(100, 500))
 * </code>
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
     * Recommended default retry policy
     */
    public static RetryPolicy defaultPolicy() {
        return new DefaultRetryPolicy();
    }

    /**
     * Create retry policy that will retry all errors.
     */
    public static RetryPolicy retryAll(int attemptLimit) {
        return RetryPolicy.attemptLimited(attemptLimit, new RetryAllPolicy());
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
     * Create retry policy that will retry `code`: isCodeForRetry(`code`) == true
     */
    public static RetryPolicy forCodes(Predicate<Integer> isCodeForRetry) {
        return new YtErrorRetryPolicy(isCodeForRetry);
    }

    /**
     * Create retry policy from old RpcFailoverPolicy
     */
    public static RetryPolicy fromRpcFailoverPolicy(RpcFailoverPolicy oldPolicy) {
        return new OldFailoverRetryPolicy(oldPolicy);
    }

    /**
     * Wrap other retry policy to limit total number of attempts.
     */
    public static RetryPolicy attemptLimited(int attemptLimit, RetryPolicy inner) {
        return new AttemptLimitedRetryPolicy(attemptLimit, inner);
    }

    public static RetryPolicy either(RetryPolicy... retryPolicies) {
        return new EitherRetryPolicy(Arrays.asList(retryPolicies));
    }

    public abstract Optional<Duration> getBackoffDuration(Throwable error, RpcOptions options);

    public void onNewAttempt() {
    }

    String getTotalRetryCountDescription() {
        return "<unknown>";
    }

    @NonNullApi
    @NonNullFields
    static class OldFailoverRetryPolicy extends RetryPolicy {
        private final RpcFailoverPolicy oldPolicy;

        OldFailoverRetryPolicy(RpcFailoverPolicy oldPolicy) {
            this.oldPolicy = oldPolicy;
        }

        @Override
        public Optional<Duration> getBackoffDuration(Throwable error, RpcOptions options) {
            boolean isRetriable;

            if (error instanceof TimeoutException) {
                isRetriable = oldPolicy.onTimeout();
            } else {
                isRetriable = oldPolicy.onError(error);
            }

            if (isRetriable) {
                return Optional.of(Duration.ZERO);
            } else {
                return Optional.empty();
            }
        }
    }

    @NonNullApi
    @NonNullFields
    static class DefaultRetryPolicy extends RetryPolicy {
        private static final HashSet CODES_FOR_RETRY = new HashSet<>(Arrays.asList(
                RpcErrorCode.TransactionLockConflict.getCode(),
                RpcErrorCode.AllWritesDisabled.getCode(),
                RpcErrorCode.TableMountInfoNotReady.getCode(),
                RpcErrorCode.TooManyRequests.getCode(),
                RpcErrorCode.RequestQueueSizeLimitExceeded.getCode(),
                RpcErrorCode.RpcRequestQueueSizeLimitExceeded.getCode(),
                RpcErrorCode.TooManyOperations.getCode(),
                RpcErrorCode.TransportError.getCode(),
                RpcErrorCode.OperationProgressOutdated.getCode(),
                RpcErrorCode.Canceled.getCode()
        ));

        private static final HashSet CHUNK_NOT_RETRIABLE_CODES = new HashSet<>(Arrays.asList(
                RpcErrorCode.SessionAlreadyExists.getCode(),
                RpcErrorCode.ChunkAlreadyExists.getCode(),
                RpcErrorCode.WindowError.getCode(),
                RpcErrorCode.BlockContentMismatch.getCode(),
                RpcErrorCode.InvalidBlockChecksum.getCode(),
                RpcErrorCode.BlockOutOfRange.getCode(),
                RpcErrorCode.MissingExtension.getCode(),
                RpcErrorCode.NoSuchBlock.getCode(),
                RpcErrorCode.NoSuchChunk.getCode(),
                RpcErrorCode.NoSuchChunkList.getCode(),
                RpcErrorCode.NoSuchChunkTree.getCode(),
                RpcErrorCode.NoSuchChunkView.getCode(),
                RpcErrorCode.NoSuchMedium.getCode()
        ));

        private final RetryPolicy inner = attemptLimited(3, RetryPolicy.forCodes(
                code -> CODES_FOR_RETRY.contains(code) || isChunkRetriableError(code)
        ));

        @Override
        public Optional<Duration> getBackoffDuration(Throwable error, RpcOptions options) {
            return inner.getBackoffDuration(error, options);
        }

        @Override
        public void onNewAttempt() {
            inner.onNewAttempt();
        }

        @Override
        public String getTotalRetryCountDescription() {
            return inner.getTotalRetryCountDescription();
        }

        private boolean isChunkRetriableError(Integer code) {
            if (CHUNK_NOT_RETRIABLE_CODES.contains(code)) {
                return false;
            }
            return code / 100 == 7;
        }
    }

    @NonNullApi
    @NonNullFields
    static class YtErrorRetryPolicy extends RetryPolicy {
        private final Predicate<Integer> isCodeForRetry;
        private final BackoffProvider backoffProvider = new BackoffProvider();

        YtErrorRetryPolicy(Collection<Integer> codesToRetry) {
            HashSet errorCodesToRetry = new HashSet<>(codesToRetry);
            this.isCodeForRetry = errorCodesToRetry::contains;
        }

        YtErrorRetryPolicy(Predicate<Integer> isCodeForRetry) {
            this.isCodeForRetry = isCodeForRetry;
        }

        @Override
        public Optional<Duration> getBackoffDuration(Throwable error, RpcOptions options) {
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
                if (rpcError.matches(isCodeForRetry)) {
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
        public Optional<Duration> getBackoffDuration(Throwable error, RpcOptions options) {
            return Optional.empty();
        }
    }

    static class RetryAllPolicy extends RetryPolicy {
        @Override
        public Optional<Duration> getBackoffDuration(Throwable error, RpcOptions options) {
            return Optional.of(Duration.ZERO);
        }
    }

    static class EitherRetryPolicy extends RetryPolicy {
        private final List<RetryPolicy> retryPolicies;

        EitherRetryPolicy(List<RetryPolicy> retryPolicies) {
            this.retryPolicies = retryPolicies;
        }

        @Override
        public Optional<Duration> getBackoffDuration(Throwable error, RpcOptions options) {
            for (RetryPolicy retryPolicy : retryPolicies) {
                Optional<Duration> backoff = retryPolicy.getBackoffDuration(error, options);
                if (backoff.isPresent()) {
                    return backoff;
                }
            }
            return Optional.empty();
        }

        @Override
        public void onNewAttempt() {
            for (RetryPolicy retryPolicy : retryPolicies) {
                retryPolicy.onNewAttempt();
            }
        }
    }
}

@NonNullApi
@NonNullFields
class BackoffProvider {
    private @Nullable
    Duration currentExponentialBackoff = null;

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
