package tech.ytsaurus.client.rpc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import tech.ytsaurus.TGuid;
import tech.ytsaurus.TGuidOrBuilder;
import tech.ytsaurus.TSerializedMessageEnvelope;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpc.TRequestCancelationHeader;
import tech.ytsaurus.rpc.TStreamingPayloadHeader;

public class RpcUtil {
    public static final long MICROS_PER_SECOND = 1_000_000L;
    public static final long NANOS_PER_MICROSECOND = 1_000L;

    private RpcUtil() {
    }

    public static byte[] createMessageHeader(RpcMessageType type, MessageLite header) {
        int size = header.getSerializedSize();
        byte[] data = new byte[4 + size];
        ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).putInt(type.getValue());
        try {
            CodedOutputStream output = CodedOutputStream.newInstance(data, 4, size);
            header.writeTo(output);
            output.checkNoSpaceLeft();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return data;
    }

    public static byte[] createMessageBodyWithCompression(MessageLite body, Compression codecId) {
        Codec codec = Codec.codecFor(codecId);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream output = CodedOutputStream.newInstance(baos);
        try {
            body.writeTo(output);
            output.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return codec.compress(baos.toByteArray());
    }

    public static int attachmentSize(byte[] attachment) {
        if (attachment == null) {
            return 1;
        } else {
            return attachment.length;
        }
    }

    public static byte[] createMessageBodyWithEnvelope(MessageLite body) {
        TSerializedMessageEnvelope header = TSerializedMessageEnvelope.getDefaultInstance();
        int headerSize = header.getSerializedSize();
        int bodySize = body.getSerializedSize();
        byte[] data = new byte[8 + headerSize + bodySize];
        ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).putInt(headerSize).putInt(bodySize);
        try {
            CodedOutputStream output = CodedOutputStream.newInstance(data, 8, data.length - 8);
            header.writeTo(output);
            body.writeTo(output);
            output.checkNoSpaceLeft();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return data;
    }


    public static <T> T parseMessageBodyWithCompression(
            byte[] data,
            Parser<T> parser,
            Compression compression
    ) {
        try {
            Codec codec = Codec.codecFor(compression);
            byte[] decompressed = codec.decompress(data);
            return parser.parseFrom(decompressed);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static <T> T parseMessageBodyWithEnvelope(
            byte[] data,
            Parser<T> parser
    ) {
        if (data == null || data.length < 8) {
            throw new IllegalStateException("Missing fixed envelope header");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data, 0, 8).order(ByteOrder.LITTLE_ENDIAN);
        int headerSize = buffer.getInt();
        int bodySize = buffer.getInt();
        if (headerSize < 0 || bodySize < 0 || 8 + headerSize + bodySize > data.length) {
            throw new IllegalStateException("Corrupted fixed envelope header");
        }
        try {
            CodedInputStream input = CodedInputStream.newInstance(data, 8, headerSize);
            TSerializedMessageEnvelope header = TSerializedMessageEnvelope.parseFrom(input);
            if (header.getCodec() != 0) {
                throw new IllegalStateException(
                        "Compression codecs are not supported: message body has codec=" + header.getCodec());
            }
            return parser.parseFrom(data, 8 + headerSize, bodySize);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static List<byte[]> createCompressedAttachments(List<byte[]> attachments, Compression codecId) {
        if (codecId == Compression.None || attachments.isEmpty()) {
            return attachments;
        } else {
            Codec codec = Codec.codecFor(codecId);
            return attachments.stream().map(codec::compress).collect(Collectors.toList());
        }
    }

    public static List<byte[]> createCancelMessage(TRequestCancelationHeader header) {
        return Collections.singletonList(createMessageHeader(RpcMessageType.CANCEL, header));
    }

    public static List<byte[]> createEofMessage(TStreamingPayloadHeader header) {
        List<byte[]> message = new ArrayList<>(2);
        message.add(createMessageHeader(RpcMessageType.STREAMING_PAYLOAD, header));
        message.add(null);
        return message;
    }

    /**
     * Returns a new CompletableFuture that is already completed exceptionally with the given exception.
     * <p>
     * NB. This is compat with JDK8 that doesn't have {@link CompletableFuture#failedFuture(Throwable)} method.
     */
    public static <T> CompletableFuture<T> failedFuture(Throwable ex) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(ex);
        return future;
    }

    /**
     * Добавляет вызов dst.cancel(false) в случае завершения src
     */
    public static <T> void relayCancel(CompletableFuture<T> src, Future<?> dst) {
        if (src.isDone()) {
            if (!dst.isDone()) {
                dst.cancel(false);
            }
        } else {
            src.whenComplete((ignoredResult, ignoredFailure) -> {
                if (!dst.isDone()) {
                    dst.cancel(false);
                }
            });
        }
    }

    /**
     * Транслирует результат применения fn.apply(value) в результат для f
     */
    public static <T, U> void relayApply(
            CompletableFuture<U> f,
            T value,
            Throwable exception,
            Function<? super T, ? extends U> fn
    ) {
        if (!f.isDone()) {
            if (exception != null) {
                f.completeExceptionally(exception);
            } else {
                try {
                    f.complete(fn.apply(value));
                } catch (Throwable e) {
                    f.completeExceptionally(e);
                }
            }
        }
    }

    /**
     * Аналогично f.thenApply(fn), но с дополнениями:
     * <p>
     * - Автоматически вызывается cancel(false) на f
     */
    public static <T, U> CompletableFuture<U> apply(CompletableFuture<T> f, Function<? super T, ? extends U> fn) {
        CompletableFuture<U> result = new CompletableFuture<>();
        if (f.isDone()) {
            // Экономим стек и вызываем функцию напрямую
            try {
                result.complete(fn.apply(f.get()));
            } catch (Throwable e) {
                result.completeExceptionally(e);
            }
        } else {
            f.whenComplete((r, e) -> relayApply(result, r, e, fn));
        }
        relayCancel(result, f);
        return result;
    }

    /**
     * Аналогично f.thenApplyAsync(fn, executor), но с дополнениями:
     * <p>
     * - Автоматически вызывается cancel(false) на f
     */
    public static <T, U> CompletableFuture<U> applyAsync(
            CompletableFuture<T> f,
            Function<? super T, ? extends U> fn,
            Executor executor
    ) {
        CompletableFuture<U> result = new CompletableFuture<>();
        f.whenCompleteAsync((r, e) -> relayApply(result, r, e, fn), executor);
        relayCancel(result, f);
        return result;
    }

    /**
     * Replacement for {@link CompletableFuture#orTimeout} that is missing in JDK 8.
     * Additionally allows to specify error message.
     */
    @Nonnull
    public static <T> CompletableFuture<T> withTimeout(
            @Nonnull CompletableFuture<T> f,
            @Nonnull String errorMessage,
            long delay,
            @Nonnull TimeUnit timeUnit,
            @Nonnull ScheduledExecutorService scheduledExecutorService
    ) {
        if (!f.isDone()) {
            ScheduledFuture<?> cancelFuture = scheduledExecutorService.schedule(
                    () -> f.completeExceptionally(new TimeoutException(errorMessage)),
                    delay,
                    timeUnit);
            f.whenComplete((result, error) -> cancelFuture.cancel(false));
        }
        return f;
    }

    /**
     * Конвертирует Duration в микросекунды для yt
     */
    public static long durationToMicros(Duration duration) {
        long micros = Math.multiplyExact(duration.getSeconds(), MICROS_PER_SECOND);
        micros = Math.addExact(micros, duration.getNano() / NANOS_PER_MICROSECOND);
        return micros;
    }

    /**
     * Конвертирует микросекунды yt в Duration
     */
    public static Duration durationFromMicros(long micros) {
        long seconds = micros / MICROS_PER_SECOND;
        long nanos = (micros % MICROS_PER_SECOND) * NANOS_PER_MICROSECOND;
        return Duration.ofSeconds(seconds, nanos);
    }

    /**
     * Конвертирует Instant в микросекунды для yt
     */
    public static long instantToMicros(Instant instant) {
        long micros = Math.multiplyExact(instant.getEpochSecond(), MICROS_PER_SECOND);
        micros = Math.addExact(micros, instant.getNano() / NANOS_PER_MICROSECOND);
        return micros;
    }

    public static TGuid toProto(GUID guid) {
        return TGuid.newBuilder().setFirst(guid.getFirst()).setSecond(guid.getSecond()).build();
    }

    public static GUID fromProto(TGuidOrBuilder guid) {
        return new GUID(guid.getFirst(), guid.getSecond());
    }
}
