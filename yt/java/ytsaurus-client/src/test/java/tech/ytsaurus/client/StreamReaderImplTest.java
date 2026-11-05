package tech.ytsaurus.client;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.Parser;
import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.TError;
import tech.ytsaurus.TGuid;
import tech.ytsaurus.client.rpc.Compression;
import tech.ytsaurus.client.rpc.RpcClientStreamControl;
import tech.ytsaurus.core.common.YTsaurusError;
import tech.ytsaurus.core.common.YTsaurusErrorCode;
import tech.ytsaurus.rpc.TStreamingPayloadHeader;
import tech.ytsaurus.rpcproxy.TRspReadTable;

public class StreamReaderImplTest {
    @Test
    public void testArrowReaderCompletesOnlyAtRpcEof() throws Exception {
        var reader = new TableReaderImpl<>(TableAttachmentReader.byteBuffer());
        var control = new StubStreamControl();
        reader.onStartStream(control);

        Assert.assertNull(reader.read());
        Assert.assertTrue(reader.canRead());
        Assert.assertFalse(reader.readyEvent().isDone());

        reader.onPayload(null, payloadHeader(), Collections.singletonList(null));
        Assert.assertEquals(8, reader.read().get(0).remaining());
        Assert.assertFalse(reader.canRead());
        Assert.assertNull(reader.read());
        Assert.assertEquals(0, control.cancelCallCount.get());
    }

    @Test
    public void testArrowPartitionReaderCompletesPendingNextAtRpcEof() throws Exception {
        var reader = new TablePartitionReaderImpl<>(new TableAttachmentByteBufferPartitionReader());
        reader.onStartStream(new StubStreamControl());
        CompletableFuture<List<ByteBuffer>> pending = reader.next();
        Assert.assertFalse(pending.isDone());

        reader.onPayload(null, payloadHeader(), Collections.singletonList(null));
        Assert.assertEquals(8, pending.get(5, TimeUnit.SECONDS).get(0).remaining());
        Assert.assertNull(reader.next().get(5, TimeUnit.SECONDS));
    }

    @Test
    public void testClosingArrowReaderDoesNotProduceTerminator() throws Exception {
        var reader = new TableReaderImpl<>(TableAttachmentReader.byteBuffer());
        reader.onStartStream(new StubStreamControl());
        reader.close();
        Assert.assertNull(reader.read());
    }

    private static TStreamingPayloadHeader payloadHeader() {
        return TStreamingPayloadHeader.newBuilder()
                .setCodec(0)
                .setRequestId(TGuid.newBuilder().setFirst(1).setSecond(1))
                .setService("test")
                .setMethod("test")
                .setSequenceNumber(0)
                .build();
    }

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
