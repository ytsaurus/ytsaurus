package ru.yandex.yt.ytclient.rpc.internal;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Objects;

import com.google.protobuf.MessageLite;

import ru.yandex.yt.ytclient.misc.YtGuid;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.rpc.TRequestHeader;

/**
 * Реализация интерфейсов
 */
public class RpcServiceClient implements InvocationHandler {
    private static final Object[] EMPTY_ARGS = new Object[0];

    private final RpcClient client;
    private final Class<?> interfaceClass;
    private final RpcServiceDescriptor serviceDescriptor;
    private final String serviceName;
    private final int protocolVersion;
    private final Duration defaultTimeout;
    private final boolean defaultRequestAck;

    private RpcServiceClient(RpcClient client, Class<?> interfaceClass, RpcOptions options) {
        this.client = Objects.requireNonNull(client);
        this.interfaceClass = Objects.requireNonNull(interfaceClass);
        this.serviceDescriptor = RpcServiceDescriptor.forInterface(interfaceClass);
        this.serviceName =
                options.getServiceName() != null ? options.getServiceName() : serviceDescriptor.getServiceName();
        this.protocolVersion = options.getProtocolVersion() != 0 ? options.getProtocolVersion()
                : serviceDescriptor.getProtocolVersion();
        this.defaultTimeout = options.getDefaultTimeout();
        this.defaultRequestAck = options.getDefaultRequestAck();
    }

    private TRequestHeader.Builder createHeader(RpcServiceMethodDescriptor methodDescriptor) {
        TRequestHeader.Builder builder = TRequestHeader.newBuilder();
        builder.setRequestId(YtGuid.create().toProto());
        builder.setService(serviceName);
        builder.setMethod(methodDescriptor.getMethodName());
        builder.setProtocolVersion(protocolVersion);
        return builder;
    }

    @SuppressWarnings("unchecked")
    private RpcClientRequestBuilder<?, ?> createOneWayBuilder(RpcServiceMethodDescriptor methodDescriptor) {
        return new RequestOneWayBuilder(client, createHeader(methodDescriptor),
                (MessageLite.Builder) methodDescriptor.getRequestBodyCreator().get());
    }

    @SuppressWarnings("unchecked")
    private RpcClientRequestBuilder<?, ?> createNormalBuilder(RpcServiceMethodDescriptor methodDescriptor) {
        return new RequestWithResponseBuilder(client, createHeader(methodDescriptor),
                (MessageLite.Builder) methodDescriptor.getRequestBodyCreator().get(),
                methodDescriptor.getResponseBodyParser());
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
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
        if (methodDescriptor.isOneWay()) {
            builder = createOneWayBuilder(methodDescriptor);
        } else {
            builder = createNormalBuilder(methodDescriptor);
        }
        builder.setTimeout(defaultTimeout);
        builder.setRequestAck(defaultRequestAck);
        return builder;
    }

    /**
     * Создаёт реализацию interfaceClass для вызова методов через client
     */
    public static <T> T create(RpcClient client, Class<T> interfaceClass) {
        return create(client, interfaceClass, new RpcOptions());
    }

    /**
     * Создаёт реализацию interfaceClass для вызова методов через client
     */
    @SuppressWarnings("unchecked")
    public static <T> T create(RpcClient client, Class<T> interfaceClass, RpcOptions options) {
        InvocationHandler handler = new RpcServiceClient(client, interfaceClass, options);
        return (T) Proxy.newProxyInstance(interfaceClass.getClassLoader(), new Class[]{interfaceClass}, handler);
    }
}
