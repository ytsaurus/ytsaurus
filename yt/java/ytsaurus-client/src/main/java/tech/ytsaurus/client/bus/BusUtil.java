package tech.ytsaurus.client.bus;

import java.nio.ByteOrder;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.Future;
import tech.ytsaurus.core.GUID;

/**
 * Полезные для работы с bus функции
 */
public final class BusUtil {
    private static final char[] DIGITS = "0123456789ABCDEF".toCharArray();

    private BusUtil() {
        // static class
    }

    /**
     * Конвертирует массив байт в hex строку, используется для логирования bus пакетов
     */
    public static void encodeHex(StringBuilder sb, byte[] bytes) {
        sb.ensureCapacity(sb.length() + bytes.length * 3);
        for (int i = 0; i < bytes.length; i++) {
            if (i != 0) {
                sb.append(' ');
            }
            int b = bytes[i] & 0xff;
            sb.append(DIGITS[b >> 4]);
            sb.append(DIGITS[b & 15]);
        }
    }

    /**
     * Отменяет future по завершении completableFuture
     */
    public static void relayCancel(CompletableFuture<?> completableFuture, Future<?> future) {
        completableFuture.whenComplete((ignoredResult, ignoredException) -> {
            if (!future.isDone() && future.isCancellable()) {
                future.cancel(false);
            }
        });
    }

    /**
     * Транслирует результат завершения src в результат dst
     */
    public static <T> void relayResult(Future<T> src, CompletableFuture<T> dst) {
        if (src.isDone()) {
            if (!dst.isDone()) {
                if (src.isSuccess()) {
                    dst.complete(src.getNow());
                } else {
                    dst.completeExceptionally(src.cause());
                }
            }
        } else {
            src.addListener(ignored -> {
                if (!dst.isDone()) {
                    if (src.isSuccess()) {
                        dst.complete(src.getNow());
                    } else {
                        dst.completeExceptionally(src.cause());
                    }
                }
            });
        }
    }

    public static <T> CompletableFuture<T> makeCompletableFuture(Future<T> future) {
        return makeCompletableFuture(future, false);
    }

    public static <T> CompletableFuture<T> makeCompletableFuture(Future<T> future, boolean relayCancel) {
        CompletableFuture<T> result = new CompletableFuture<>();
        relayResult(future, result);
        if (relayCancel) {
            relayCancel(result, future);
        }
        return result;
    }

    public static void writeTo(ByteBuf out, GUID guid) {
        out = out.order(ByteOrder.LITTLE_ENDIAN);
        out.writeLong(guid.getFirst());
        out.writeLong(guid.getSecond());
    }

    public static GUID readGuidFrom(ByteBuf in) {
        if (in.readableBytes() < 16) {
            throw new IllegalArgumentException("At least 16 bytes must be readable");
        }
        in = in.order(ByteOrder.LITTLE_ENDIAN);
        long first = in.readLong();
        long second = in.readLong();
        return new GUID(first, second);
    }
}
