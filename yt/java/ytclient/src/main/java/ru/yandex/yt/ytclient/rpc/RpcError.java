package ru.yandex.yt.ytclient.rpc;

import javax.annotation.Nullable;

import ru.yandex.lang.NonNullApi;
import ru.yandex.yson.YsonConsumer;
import ru.yandex.yson.YsonError;
import ru.yandex.yson.YsonParser;
import ru.yandex.yson.YsonTextWriter;
import ru.yandex.yt.TError;
import ru.yandex.yt.ytree.TAttribute;

@NonNullApi
public class RpcError extends RuntimeException {
    private final TError error;

    public RpcError(TError error) {
        super(createFullErrorDescription(error));
        this.error = error;
    }

    public TError getError() {
        return error;
    }

    /**
     * Check if error or one of inner error has specified code.
     */
    public boolean matches(int code) {
        return findMatchingError(code) != null;
    }

    /**
     * Returns error of one of the inner error which has specified code.
     * Returns null if no such error is found.
     */
    @Nullable
    public TError findMatchingError(int code) {
        return findMatching(error, code);
    }

    /**
     * Prefer to use  {@link #findMatchingError(int)}.
     */
    @Nullable
    @Deprecated
    public RpcError findMatching(int code) {
        if (error.getCode() == code) {
            return this;
        }
        TError matching = findMatching(error, code);
        if (matching == null) {
            return null;
        }
        return new RpcError(matching);
    }

    public boolean isUnrecoverable() {
        int code = error.getCode();
        return code == RpcErrorCode.Timeout.code ||
                code == RpcErrorCode.ProxyBanned.code ||
                code == RpcErrorCode.TransportError.code ||
                code == RpcErrorCode.Unavailable.code ||
                code == RpcErrorCode.NoSuchService.code ||
                code == RpcErrorCode.NoSuchMethod.code ||
                code == RpcErrorCode.ProtocolError.code;
    }

    public static boolean isUnrecoverable(Throwable e) {
        return !(e instanceof RpcError) || ((RpcError) e).isUnrecoverable();
    }

    public static String createFullErrorDescription(TError error) {
        StringBuilder sb = new StringBuilder();
        writeShortErrorDescription(error, sb);
        sb.append("; full error: ");
        try {
            serializeError(error, new YsonTextWriter(sb));
        } catch (YsonError ex) {
            sb.append("<yson parsing error occurred>");
        }
        return sb.toString();
    }

    @Nullable
    private static TError findMatching(@Nullable TError error, int code) {
        if (error == null) {
            return null;
        } else if (error.getCode() == code) {
            return error;
        }

        TError result = null;
        for (TError inner : error.getInnerErrorsList()) {
            result = findMatching(inner, code);
            if (result != null) {
                break;
            }
        }
        return result;
    }

    private static void writeShortErrorDescription(TError error, StringBuilder sb) {
        while (true) {
            sb.append('\'').append(error.getMessage()).append('\'');
            if (error.getInnerErrorsCount() > 0) {
                sb.append(" <== ");
                // Usually we have no more than one inner error.
                // If there are many of them we output only the first one in short message description to ease reading.
                // All other inner errors will also printed in "full error:" section.
                error = error.getInnerErrors(0);
            } else {
                break;
            }
        }
    }

    private static void serializeError(TError error, YsonConsumer consumer) {
        consumer.onBeginMap();
        {
            consumer.onKeyedItem("code");
            consumer.onInteger(error.getCode());

            consumer.onKeyedItem("message");
            consumer.onString(error.getMessage());

            if (error.getAttributes().getAttributesCount() != 0) {
                consumer.onKeyedItem("attributes");
                consumer.onBeginMap();
                {
                    for (TAttribute attribute : error.getAttributes().getAttributesList()) {
                        consumer.onKeyedItem(attribute.getKey());
                        new YsonParser(attribute.getValue().toByteArray()).parseNode(consumer);
                    }
                }
                consumer.onEndMap();
            }

            if (error.getInnerErrorsCount() > 0) {
                consumer.onKeyedItem("inner_errors");
                {
                    consumer.onBeginList();
                    for (TError innerError : error.getInnerErrorsList()) {
                        serializeError(innerError, consumer);
                    }
                    consumer.onEndList();
                }
            }
        }
        consumer.onEndMap();
    }
}
