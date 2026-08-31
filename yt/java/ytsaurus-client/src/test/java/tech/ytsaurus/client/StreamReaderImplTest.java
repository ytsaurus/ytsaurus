package tech.ytsaurus.client;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.Parser;
import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.TError;
import tech.ytsaurus.client.rpc.Compression;
import tech.ytsaurus.client.rpc.RpcClientStreamControl;
import tech.ytsaurus.core.common.YTsaurusError;
import tech.ytsaurus.core.common.YTsaurusErrorCode;
import tech.ytsaurus.rpcproxy.TRspReadTable;

public class StreamReaderImplTest {
    @Test
    public void testRequestCanceledAfterCloseCompletesPendingRead() {
        StreamReaderImpl<TRspReadTable> reader = new StreamReaderImpl<>() {
            @Override
            protected Parser<TRspReadTable> responseParser() {
                return TRspReadTable.parser();
            }
        };
        StubStreamControl control = new StubStreamControl();
        reader.onStartStream(control);

        CompletableFuture<Void> readyEvent = reader.getReadyEvent();
        CompletableFuture<Void> closeFuture = reader.doClose();
        reader.onError(new YTsaurusError(TError.newBuilder()
                .setCode(YTsaurusErrorCode.Canceled.code)
                .setMessage("Request canceled")
                .build()));

        readyEvent.join();
        closeFuture.join();
        Assert.assertEquals(1, control.cancelCallCount.get());
        Assert.assertFalse(reader.result.isCompletedExceptionally());
    }

    @Test
    public void testRequestCanceledAfterAsyncReaderCloseCompletesPendingNext() {
        AsyncTableReaderImpl<byte[]> reader = new AsyncTableReaderImpl<>(TableAttachmentReader.byPass());
        reader.onStartStream(new StubStreamControl());

        CompletableFuture<List<byte[]>> pending = reader.next();
        reader.close();
        reader.onError(new YTsaurusError(TError.newBuilder()
                .setCode(YTsaurusErrorCode.Canceled.code)
                .setMessage("Request canceled")
                .build()));

        Assert.assertNull(pending.join());
    }

    private static class StubStreamControl implements RpcClientStreamControl {
        private final AtomicInteger cancelCallCount = new AtomicInteger();

        @Override
        public Compression getExpectedPayloadCompression() {
            return Compression.None;
        }

        @Override
        public CompletableFuture<Void> feedback(long offset) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> sendEof() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> sendPayload(List<byte[]> attachments) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void wakeUp() {
        }

        @Override
        public String getRpcProxyAddress() {
            return "test-proxy";
        }

        @Override
        public boolean cancel() {
            cancelCallCount.incrementAndGet();
            return true;
        }
    }
}
