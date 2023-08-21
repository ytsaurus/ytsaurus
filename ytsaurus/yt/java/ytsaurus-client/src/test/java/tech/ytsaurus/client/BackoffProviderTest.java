package tech.ytsaurus.client;

import java.time.Duration;

import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.TError;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.core.common.YTsaurusError;

public class BackoffProviderTest {
    static final int REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED = 904;

    static YTsaurusError rpcErrorWithCodes(int... codes) {
        TError error = null;
        if (codes.length == 0) {
            error = TError.newBuilder()
                    .setCode(1)
                    .setMessage("Error!")
                    .build();
        }

        for (int code : codes) {
            var outerError = TError.newBuilder()
                    .setCode(code)
                    .setMessage("Error!");
            if (error != null) {
                outerError.addInnerErrors(error);
            }
            error = outerError.build();
        }

        return new YTsaurusError(error);
    }

    @Test
    public void testExponentialBackoff() {
        var backoffProvider = new BackoffProvider();
        var options = new RpcOptions();
        options.setMinBackoffTime(Duration.ofSeconds(2));
        options.setMaxBackoffTime(Duration.ofSeconds(10));

        Assert.assertEquals(
                backoffProvider.getBackoffTime(rpcErrorWithCodes(1, REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED), options),
                Duration.ofSeconds(2)
        );

        Assert.assertEquals(
                backoffProvider.getBackoffTime(rpcErrorWithCodes(2, REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED), options),
                Duration.ofSeconds(4)
        );

        Assert.assertEquals(
                backoffProvider.getBackoffTime(rpcErrorWithCodes(2, REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED), options),
                Duration.ofSeconds(8)
        );

        Assert.assertEquals(
                backoffProvider.getBackoffTime(rpcErrorWithCodes(2, 5, REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED), options),
                Duration.ofSeconds(10)
        );
    }

    @Test
    public void testExponentialBackoffZeroMinBackoffTime() {
        var backoffProvider = new BackoffProvider();
        var options = new RpcOptions();
        options.setMinBackoffTime(Duration.ofSeconds(0));
        options.setMaxBackoffTime(Duration.ofSeconds(5));

        Assert.assertEquals(
                backoffProvider.getBackoffTime(rpcErrorWithCodes(1, REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED), options),
                Duration.ofSeconds(0)
        );

        Assert.assertEquals(
                backoffProvider.getBackoffTime(rpcErrorWithCodes(2, REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED), options),
                Duration.ofSeconds(1)
        );

        Assert.assertEquals(
                backoffProvider.getBackoffTime(rpcErrorWithCodes(2, REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED), options),
                Duration.ofSeconds(2)
        );

        Assert.assertEquals(
                backoffProvider.getBackoffTime(rpcErrorWithCodes(2, 5, REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED), options),
                Duration.ofSeconds(4)
        );
        Assert.assertEquals(
                backoffProvider.getBackoffTime(rpcErrorWithCodes(2, 5, REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED), options),
                Duration.ofSeconds(5)
        );
    }
}
