package tech.ytsaurus.core.utils;

import java.lang.reflect.InvocationTargetException;

import com.google.protobuf.Message;

public class ProtoUtils {
    private ProtoUtils() {
    }

    public static Message.Builder newBuilder(Class<? extends Message> messageClass) {
        return invokeOnMessageClass(messageClass, "newBuilder");
    }

    @SuppressWarnings("unchecked")
    private static <T> T invokeOnMessageClass(Class<? extends Message> messageClass, String methodName) {
        try {
            return (T) messageClass.getMethod(methodName).invoke(null);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException ex) {
            throw new RuntimeException(ex);
        }
    }
}
