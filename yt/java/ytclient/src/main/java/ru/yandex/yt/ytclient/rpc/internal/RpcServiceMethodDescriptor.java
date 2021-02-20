package ru.yandex.yt.ytclient.rpc.internal;

import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.function.Supplier;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.annotations.RpcMethod;

/**
 * Описание метода на основе рефлексии
 */
public class RpcServiceMethodDescriptor {
    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();

    private final Method method;
    private final String methodName;
    private final Class<?> requestBodyType;
    private final Supplier<?> requestBodyCreator;
    private final Class<?> responseBodyType;
    private final Parser<?> responseBodyParser;

    public RpcServiceMethodDescriptor(Method method) {
        this.method = method;
        if (method.getParameterCount() != 0) {
            throw new IllegalArgumentException("RPC methods must not have any parameters: " + method);
        }

        RpcMethod methodAnnotation = method.getAnnotation(RpcMethod.class);
        if (methodAnnotation != null && !methodAnnotation.name().isEmpty()) {
            this.methodName = methodAnnotation.name();
        } else {
            String name = method.getName();
            if (!Character.isTitleCase(name.charAt(0))) {
                name = Character.toTitleCase(name.charAt(0)) + name.substring(1);
            }
            this.methodName = name;
        }

        ParameterizedType requestBuilderType = unpackRequestBuilderType(method.getGenericReturnType());
        Type[] args = requestBuilderType.getActualTypeArguments();
        if (args.length != 2 || !(args[0] instanceof Class)) {
            throw new IllegalArgumentException("Cannot determine request/response type from " + requestBuilderType);
        }

        this.requestBodyType = (Class<?>) args[0];
        this.requestBodyCreator = makeBuilderCreator(requestBodyType);

        if (args[1] instanceof ParameterizedType) {
            ParameterizedType responseType = (ParameterizedType) args[1];
            Type[] responseArgs = responseType.getActualTypeArguments();
            if (responseType.getRawType() == RpcClientResponse.class && responseArgs.length == 1
                    && responseArgs[0] instanceof Class && MessageLite.class
                    .isAssignableFrom((Class<?>) responseArgs[0]))
            {
                this.responseBodyType = (Class<?>) responseArgs[0];
                this.responseBodyParser = makeMessageParser(responseBodyType);
            } else {
                throw new IllegalArgumentException("Unsupported response type: " + args[1]);
            }
        } else {
            throw new IllegalArgumentException("Unsupported response type: " + args[1]);
        }
    }

    public Method getMethod() {
        return method;
    }

    public String getMethodName() {
        return methodName;
    }

    public Class<?> getRequestBodyType() {
        return requestBodyType;
    }

    public Supplier<?> getRequestBodyCreator() {
        return requestBodyCreator;
    }

    public Class<?> getResponseBodyType() {
        return responseBodyType;
    }

    public Parser<?> getResponseBodyParser() {
        return responseBodyParser;
    }

    private static ParameterizedType unpackRequestBuilderType(Type type) {
        if (type instanceof ParameterizedType) {
            ParameterizedType ptype = (ParameterizedType) type;
            if (ptype.getRawType() == RpcClientRequestBuilder.class) {
                return ptype;
            }
        }
        throw new IllegalArgumentException(
                "RPC methods must return RpcClientRequestBuilder<Request, Response>, not " + type);
    }

    @SuppressWarnings("unchecked")
    private static <T> Supplier<T> makeBuilderCreator(Class<T> builderClass) {
        Class<?> messageClass = builderClass.getEnclosingClass();
        if (messageClass == null || !MessageLite.class.isAssignableFrom(messageClass)) {
            throw new IllegalArgumentException(
                    "Class " + builderClass.getName() + " does not have an enclosing message type");
        }
        try {
            MethodType methodType = MethodType.methodType(builderClass);
            MethodHandle handle = LOOKUP.findStatic(messageClass, "newBuilder", methodType);
            return (Supplier<T>) LambdaMetafactory.metafactory(
                    LOOKUP,
                    "get",
                    MethodType.methodType(Supplier.class),
                    methodType.generic(),
                    handle,
                    methodType).getTarget().invokeExact();
        } catch (Throwable e) {
            throw new IllegalArgumentException("Cannot build lambda for the newBuilder() method in " + messageClass, e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> Parser<T> makeMessageParser(Class<T> messageClass) {
        MethodHandle handle;
        try {
            MethodType methodType = MethodType.methodType(Parser.class);
            handle = LOOKUP.findStatic(messageClass, "parser", methodType);
            return (Parser<T>)handle.invoke();
        } catch (Throwable e) {
            throw new IllegalArgumentException("Cannot find parser in " + messageClass, e);
        }
    }
}
