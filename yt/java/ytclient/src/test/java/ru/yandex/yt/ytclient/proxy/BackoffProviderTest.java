package ru.yandex.yt.ytclient.proxy;

import java.time.Duration;

import org.junit.Test;

import ru.yandex.yt.TError;
import ru.yandex.yt.ytclient.rpc.RpcError;
import ru.yandex.yt.ytclient.rpc.RpcOptions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class BackoffProviderTest {
    static final int REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED = 904;

    static RpcError rpcErrorWithCodes(int... codes) {
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

        return new RpcError(error);
    }

    @Test
    public void testExponentialBackoff() {
        var backoffProvider = new BackoffProvider();
        var options = new RpcOptions();
        options.setMinBackoffTime(Duration.ofSeconds(2));
        options.setMaxBackoffTime(Duration.ofSeconds(10));

        assertThat(
                backoffProvider.getBackoffTime(rpcErrorWithCodes(1, REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED), options),
                is(Duration.ofSeconds(2))
        );

        assertThat(
                backoffProvider.getBackoffTime(rpcErrorWithCodes(2, REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED), options),
                is(Duration.ofSeconds(4))
        );

        assertThat(
                backoffProvider.getBackoffTime(rpcErrorWithCodes(2, REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED), options),
                is(Duration.ofSeconds(8))
        );

        assertThat(
                backoffProvider.getBackoffTime(rpcErrorWithCodes(2, 5, REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED), options),
                is(Duration.ofSeconds(10))
        );
    }

    @Test
    public void testExponentialBackoffZeroMinBackoffTime() {
        var backoffProvider = new BackoffProvider();
        var options = new RpcOptions();
        options.setMinBackoffTime(Duration.ofSeconds(0));
        options.setMaxBackoffTime(Duration.ofSeconds(5));

        assertThat(
                backoffProvider.getBackoffTime(rpcErrorWithCodes(1, REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED), options),
                is(Duration.ofSeconds(0))
        );

        assertThat(
                backoffProvider.getBackoffTime(rpcErrorWithCodes(2, REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED), options),
                is(Duration.ofSeconds(1))
        );

        assertThat(
                backoffProvider.getBackoffTime(rpcErrorWithCodes(2, REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED), options),
                is(Duration.ofSeconds(2))
        );

        assertThat(
                backoffProvider.getBackoffTime(rpcErrorWithCodes(2, 5, REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED), options),
                is(Duration.ofSeconds(4))
        );
        assertThat(
                backoffProvider.getBackoffTime(rpcErrorWithCodes(2, 5, REQUEST_QUEUE_SIZE_LIMIT_EXCEEDED), options),
                is(Duration.ofSeconds(5))
        );
    }
}
