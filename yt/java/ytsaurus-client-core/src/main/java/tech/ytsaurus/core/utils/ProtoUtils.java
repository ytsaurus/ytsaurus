package tech.ytsaurus.core.utils;

import java.lang.reflect.InvocationTargetException;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;

public class ProtoUtils {
    private ProtoUtils() {
    }

    public static Message.Builder newBuilder(Class<? extends Message> messageClass) {
        return invokeOnMessageClass(messageClass, "newBuilder", null, null);
    }

    public static <T extends MessageLite> T parseFrom(Class<? extends MessageLite> messageClass, ByteString bytes) {
        return invokeOnMessageClass(
                messageClass,
                "parseFrom",
                new Class[]{ByteString.class},
                new Object[]{bytes}
        );
    }

    @SuppressWarnings("unchecked")
    private static <T> T invokeOnMessageClass(
            Class<? extends MessageLite> messageClass,
            String methodName,
            @Nullable Class<?>[] argumentTypes,
            @Nullable Object[] arguments
    ) {
        try {
            return (T) messageClass.getMethod(methodName, argumentTypes).invoke(null, arguments);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException ex) {
            throw new RuntimeException(ex);
        }
    }
}
