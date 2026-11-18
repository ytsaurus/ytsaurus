package tech.ytsaurus.client;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionException;

import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.core.common.YTsaurusErrorCode;

import static tech.ytsaurus.client.BackoffProviderTest.rpcErrorWithCodes;

public class TableWriterRetryPolicyTest {
    static final int ALL_TARGET_NODES_FAILED = 700;
    static final int CONCURRENT_TRANSACTION_LOCK_CONFLICT = 402;
    static final Duration MIN_BACKOFF = Duration.ofSeconds(2);
    static final Duration LONG_BACKOFF = Duration.ofSeconds(60);

    private final RpcOptions options = new RpcOptions().setMinBackoffTime(MIN_BACKOFF);

    @Test
    public void testChunkErrorsAndRateLimitsGetLongBackoff() {
        Assert.assertEquals(
                Optional.of(LONG_BACKOFF),
                getFirstBackoff(rpcErrorWithCodes(YTsaurusErrorCode.TransportError.getCode(), ALL_TARGET_NODES_FAILED))
        );
        Assert.assertEquals(
                Optional.of(LONG_BACKOFF),
                getFirstBackoff(rpcErrorWithCodes(YTsaurusErrorCode.RequestQueueSizeLimitExceeded.getCode()))
        );
    }

    @Test
    public void testExcludedChunkCodeDisablesOnlyChunkRetry() {
        Assert.assertEquals(
                Optional.empty(),
                getFirstBackoff(rpcErrorWithCodes(ALL_TARGET_NODES_FAILED, YTsaurusErrorCode.WindowError.getCode()))
        );
        Assert.assertEquals(
                Optional.of(MIN_BACKOFF),
                getFirstBackoff(rpcErrorWithCodes(
                        YTsaurusErrorCode.TransportError.getCode(),
                        ALL_TARGET_NODES_FAILED,
                        YTsaurusErrorCode.WindowError.getCode()))
        );
    }

    @Test
    public void testErrorInsideCompletionException() {
        Assert.assertEquals(
                Optional.of(MIN_BACKOFF),
                getFirstBackoff(new CompletionException(rpcErrorWithCodes(YTsaurusErrorCode.Unavailable.getCode())))
        );
        Assert.assertEquals(
                Optional.of(MIN_BACKOFF),
                getFirstBackoff(new CompletionException(new IllegalStateException()))
        );
        Assert.assertEquals(
                Optional.empty(),
                getFirstBackoff(new CompletionException(rpcErrorWithCodes(CONCURRENT_TRANSACTION_LOCK_CONFLICT)))
        );
    }

    private Optional<Duration> getFirstBackoff(Throwable error) {
        RetryPolicy policy = RetryPolicy.tableWriterPolicy();
        policy.onNewAttempt();
        return policy.getBackoffDuration(error, options);
    }
}
