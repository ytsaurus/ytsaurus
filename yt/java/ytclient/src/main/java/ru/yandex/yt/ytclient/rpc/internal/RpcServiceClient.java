package ru.yandex.yt.ytclient.rpc.internal;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.ThreadLocalRandom;

import com.google.protobuf.MessageLite;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.tracing.TTracingExt;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

/**
 * Реализация интерфейсов
 */
public class RpcServiceClient implements InvocationHandler {
    private static final Object[] EMPTY_ARGS = new Object[0];

    private final RpcServiceDescriptor serviceDescriptor;
    private final String serviceName;
    private final int protocolVersion;
    private final RpcOptions options;

    private RpcServiceClient(Class<?> interfaceClass, RpcOptions options) {
        this.serviceDescriptor = RpcServiceDescriptor.forInterface(interfaceClass);
        this.serviceName = serviceDescriptor.getServiceName();
        this.protocolVersion = serviceDescriptor.getProtocolVersion();
        this.options = options;
    }

    private TRequestHeader.Builder createHeader(RpcServiceMethodDescriptor methodDescriptor) {
        TRequestHeader.Builder builder = TRequestHeader.newBuilder();
        builder.setRequestId(RpcUtil.toProto(GUID.create()));
        builder.setService(serviceName);
        builder.setMethod(methodDescriptor.getMethodName());
        builder.setProtocolVersionMajor(protocolVersion);

        if (options.getTrace()) {
            TTracingExt.Builder tracing = TTracingExt.newBuilder();
            tracing.setSampled(options.getTraceSampled());
            tracing.setDebug(options.getTraceDebug());
            tracing.setTraceId(RpcUtil.toProto(GUID.create()));
            tracing.setSpanId(ThreadLocalRandom.current().nextLong());
            builder.setExtension(TRequestHeader.tracingExt, tracing.build());
        }

        return builder;
    }

    @SuppressWarnings("unchecked")
    private RpcClientRequestBuilder<?, ?> createBuilder(RpcServiceMethodDescriptor methodDescriptor, RpcOptions options) {
        return new RequestWithResponseBuilder(createHeader(methodDescriptor),
                (MessageLite.Builder) methodDescriptor.getRequestBodyCreator().get(),
                methodDescriptor.getResponseBodyParser(), options);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
        if (args == null) {
            args = EMPTY_ARGS;
        }
        if (method.getDeclaringClass() == Object.class) {
            if (args.length == 0 && "toString".equals(method.getName())) {
                return proxy.getClass().getName() + "@" + Integer.toHexString(System.identityHashCode(proxy));
            }
            if (args.length == 0 && "hashCode".equals(method.getName())) {
                return System.identityHashCode(proxy);
            }
            if (args.length == 1 && "equals".equals(method.getName())) {
                return proxy == args[0];
            }
            throw new IllegalStateException("Unexpected method: " + method);
        }
        RpcServiceMethodDescriptor methodDescriptor = serviceDescriptor.getMethodMap().get(method);
        if (methodDescriptor == null || args.length > 0) {
            throw new IllegalStateException("Unimplemented method: " + method);
        }
        RpcClientRequestBuilder<?, ?> builder;
        builder = createBuilder(methodDescriptor, options);
        return builder;
    }

    /**
     * Создаёт реализацию interfaceClass для вызова методов через client
     */
    public static <T> T create(Class<T> interfaceClass) {
        return create(interfaceClass, new RpcOptions());
    }

    public static <T> T create(Class<T> interfaceClass, RpcOptions options) {
        InvocationHandler handler = new RpcServiceClient(interfaceClass, options);
        //noinspection unchecked
        return (T) Proxy.newProxyInstance(interfaceClass.getClassLoader(), new Class[]{interfaceClass}, handler);
    }
}
