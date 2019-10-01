package ru.yandex.yt.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogProxy<T> implements InvocationHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogProxy.class);

    private final T object;
    private final Class<T> clazz;

    private LogProxy(Class<T> interfaceClass, T object) {
        if (!interfaceClass.isInterface()) {
            throw new IllegalStateException("Class " + interfaceClass.getName() + " is not an interface");
        }
        clazz = interfaceClass;
        this.object = object;
    }

    @SuppressWarnings("unchecked")
    private T getProxy() {
        return (T) Proxy.newProxyInstance(clazz.getClassLoader(), new Class<?>[]{clazz}, this);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        try {
            final Object ret = method.invoke(object, args);
            LOGGER.info("Call class: {}, Method: {}, Args: {}, Return: {}",
                    clazz.getName(), method.getName(), Arrays.toString(args), ret);
            return ret;
        } catch (Throwable t) {
            LOGGER.info("Call class: {}, Method: {}, Args: {}",
                    clazz.getName(), method.getName(), Arrays.toString(args));
            LOGGER.error("Unexpected exception", t);
            throw t;
        }
    }

    //

    static <T> T wrap(Class<T> interfaceClass, T object) {
        return new LogProxy<T>(interfaceClass, object).getProxy();
    }
}
