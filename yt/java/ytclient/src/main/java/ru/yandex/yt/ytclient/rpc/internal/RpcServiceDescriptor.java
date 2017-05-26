package ru.yandex.yt.ytclient.rpc.internal;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import ru.yandex.yt.ytclient.rpc.annotations.RpcService;

/**
 * Описание сервиса на основе рефлексии
 */
public class RpcServiceDescriptor {
    private static final ConcurrentHashMap<Class<?>, RpcServiceDescriptor> cache = new ConcurrentHashMap<>();

    private final String serviceName;
    private final int protocolVersion;
    private final List<RpcServiceMethodDescriptor> methodList = new ArrayList<>();
    private final Map<Method, RpcServiceMethodDescriptor> methodMap = new HashMap<>();

    public RpcServiceDescriptor(Class<?> interfaceClass) {
        if (!interfaceClass.isInterface()) {
            throw new IllegalArgumentException("Not an interface: " + interfaceClass.getSimpleName());
        }
        RpcService serviceAnnotation = interfaceClass.getAnnotation(RpcService.class);
        if (serviceAnnotation != null && !serviceAnnotation.name().isEmpty()) {
            this.serviceName = serviceAnnotation.name();
        } else {
            this.serviceName = interfaceClass.getSimpleName();
        }
        this.protocolVersion = serviceAnnotation != null ? serviceAnnotation.protocolVersion() : 0;
        for (Method method : interfaceClass.getMethods()) {
            int modifiers = method.getModifiers();
            if (Modifier.isStatic(modifiers)) {
                // Пропускаем статические методы
                continue;
            }
            if (!Modifier.isAbstract(modifiers)) {
                if (method.getDeclaringClass() == Object.class) {
                    // Пропускаем методы в классе Object
                    continue;
                }
                throw new IllegalArgumentException("Non-abstract method found: " + method);
            }
            if (!Modifier.isPublic(modifiers)) {
                throw new IllegalArgumentException("Non-public method found: " + method);
            }
            RpcServiceMethodDescriptor methodDescriptor = new RpcServiceMethodDescriptor(method);
            methodList.add(methodDescriptor);
            methodMap.put(method, methodDescriptor);
        }
    }

    public String getServiceName() {
        return serviceName;
    }

    public int getProtocolVersion() {
        return protocolVersion;
    }

    public List<RpcServiceMethodDescriptor> getMethodList() {
        return Collections.unmodifiableList(methodList);
    }

    public Map<Method, RpcServiceMethodDescriptor> getMethodMap() {
        return Collections.unmodifiableMap(methodMap);
    }

    public static RpcServiceDescriptor forInterface(Class<?> interfaceClass) {
        // Сначала делаем оптимистичный get, т.к. это быстрее, чем computeIfAbsent
        RpcServiceDescriptor descriptor = cache.get(interfaceClass);
        if (descriptor == null) {
            descriptor = cache.computeIfAbsent(interfaceClass, RpcServiceDescriptor::new);
        }
        return descriptor;
    }
}
