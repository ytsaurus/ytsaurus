package ru.yandex.yt.ytclient.rpc;

import java.util.Arrays;

import com.google.protobuf.ByteString;

import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeBinarySerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.yt.TError;
import ru.yandex.yt.ytree.TAttribute;
import ru.yandex.yt.ytree.TAttributeDictionary;

public class RpcError extends RuntimeException {
    private final TError error;

    public RpcError(TError error) {
        super(errorMessage(error));
        this.error = error;
        for (TError innerError : error.getInnerErrorsList()) {
            addSuppressed(new RpcError(innerError));
        }
    }

    public static boolean isUnrecoverable(Throwable e) {
        return !(e instanceof RpcError) || ((RpcError) e).isUnrecoverable();
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

    public TError getError() {
        return error;
    }

    public RpcError findMatching(int code) {
        if (error.getCode() == code) {
            return this;
        }

        for (Throwable e : getSuppressed()) {
            RpcError matching;
            if (e instanceof RpcError && (matching = ((RpcError)e).findMatching(code)) != null) {
                return matching;
            }
        }

        return null;
    }

    private static YTreeNode parseByteString(ByteString byteString) {
        return YTreeBinarySerializer.deserialize(byteString.newInput());
    }

    private static String errorMessage(TError error) {
        StringBuilder sb = new StringBuilder();
        sb.append("Error ").append(error.getCode());
        String message = error.getMessage();
        if (message.length() > 0) {
            sb.append(": ").append(message);
        }
        TAttributeDictionary attributes = error.getAttributes();
        if (attributes.getAttributesCount() > 0) {
            sb.append(" {");
            for (int index = 0; index < attributes.getAttributesCount(); index++) {
                if (index != 0) {
                    sb.append("; ");
                }
                TAttribute attr = attributes.getAttributes(index);
                sb.append(attr.getKey()).append('=');
                ByteString rawValue = attr.getValue();
                try {
                    sb.append(parseByteString(rawValue).toString());
                } catch (RuntimeException e) {
                    sb.append("<failed to parse ").append(Arrays.toString(rawValue.toByteArray())).append(">");
                }
            }
            sb.append("}");
        }
        return sb.toString();
    }
}
