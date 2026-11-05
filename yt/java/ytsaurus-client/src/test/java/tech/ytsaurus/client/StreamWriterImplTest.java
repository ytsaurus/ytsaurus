package tech.ytsaurus.client;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.TGuid;
import tech.ytsaurus.client.rpc.RpcClient;
import tech.ytsaurus.client.rpc.RpcClientStreamControl;
import tech.ytsaurus.client.rpc.RpcStreamConsumer;
import tech.ytsaurus.rpc.TStreamingPayloadHeader;

public class StreamWriterImplTest {

    @Test
    public void testOnPayloadBeforeOnStartStream_doesNotThrowNPE() throws Exception {
        ShuffleDataWriterImpl writer = new ShuffleDataWriterImpl(
                16 * 1024 * 1024, 1024 * 1024, "pc");

        CountDownLatch payloadDelivered = new CountDownLatch(1);
        CountDownLatch allowOnStartStream = new CountDownLatch(1);

        RpcStreamConsumer delayedConsumer = new RpcStreamConsumer() {
            @Override
            public void onStartStream(RpcClientStreamControl control) {
                try {
                    payloadDelivered.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                writer.onStartStream(control);
                allowOnStartStream.countDown();
            }

            @Override
            public void onPayload(RpcClient sender, TStreamingPayloadHeader header, List<byte[]> attachments) {
                writer.onPayload(sender, header, attachments);
            }

            @Override
            public void onFeedback(RpcClient sender,
                                   tech.ytsaurus.rpc.TStreamingFeedbackHeader header,
                                   List<byte[]> attachments) {
                writer.onFeedback(sender, header, attachments);
            }

            @Override
            public void onResponse(RpcClient sender,
                                   tech.ytsaurus.rpc.TResponseHeader header,
                                   List<byte[]> attachments) {
                writer.onResponse(sender, header, attachments);
            }

            @Override
            public void onError(Throwable error) {
                writer.onError(error);
            }

            @Override
            public void onCancel(CancellationException cancel) {
                writer.onCancel(cancel);
            }

            @Override
            public void onWakeup() {
                writer.onWakeup();
            }
        };

        TStreamingPayloadHeader payloadHeader = TStreamingPayloadHeader.newBuilder()
                .setCodec(0)
                .setRequestId(TGuid.newBuilder().setFirst(1).setSecond(1).build())
                .setService("test")
                .setMethod("test")
                .setSequenceNumber(0)
                .build();

        StubStreamControl control = new StubStreamControl();
        ExecutorService pool = Executors.newFixedThreadPool(2);

        pool.submit(() -> {
            writer.onPayload(null, payloadHeader, List.of(new byte[]{1, 2, 3}));
            payloadDelivered.countDown();
        });

        pool.submit(() -> {
            delayedConsumer.onStartStream(control);
        });

        Assert.assertTrue("onStartStream should complete",
                allowOnStartStream.await(5, TimeUnit.SECONDS));

        pool.shutdown();
        Assert.assertTrue("pool should terminate", pool.awaitTermination(5, TimeUnit.SECONDS));

        Assert.assertFalse("writer result should not be completed exceptionally",
                writer.result.isCompletedExceptionally());

        Assert.assertTrue("feedback should have been called for the early onPayload",
                control.feedbackCallCount.get() > 0);
        Assert.assertTrue("feedback offset should be positive",
                control.lastFeedbackOffset.get() > 0);
    }

    private static class StubStreamControl implements RpcClientStreamControl {
        final AtomicInteger feedbackCallCount = new AtomicInteger(0);
        final AtomicLong lastFeedbackOffset = new AtomicLong(-1);

        @Override
        public tech.ytsaurus.client.rpc.Compression getExpectedPayloadCompression() {
            return tech.ytsaurus.client.rpc.Compression.None;
        }

        @Override
        public CompletableFuture<Void> feedback(long offset) {
            feedbackCallCount.incrementAndGet();
            lastFeedbackOffset.set(offset);
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
            return false;
        }
    }
}
