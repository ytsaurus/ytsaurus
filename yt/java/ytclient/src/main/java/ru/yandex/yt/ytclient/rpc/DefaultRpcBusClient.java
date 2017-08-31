package ru.yandex.yt.ytclient.rpc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.protobuf.CodedInputStream;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.rpc.TRequestCancelationHeader;
import ru.yandex.yt.rpc.TRequestHeaderOrBuilder;
import ru.yandex.yt.rpc.TResponseHeader;
import ru.yandex.yt.ytclient.bus.Bus;
import ru.yandex.yt.ytclient.bus.BusDeliveryTracking;
import ru.yandex.yt.ytclient.bus.BusFactory;
import ru.yandex.yt.ytclient.bus.BusListener;
import ru.yandex.yt.ytclient.misc.YtGuid;

/**
 * Базовая реализация rpc клиента поверх bus
 */
public class DefaultRpcBusClient implements RpcClient {
    private static final Logger logger = LoggerFactory.getLogger(DefaultRpcBusClient.class);
    private static final MetricRegistry metrics = SharedMetricRegistries.getOrCreate("ytclient");
    private static final Histogram requestsAckHistogram = metrics.histogram(MetricRegistry.name(DefaultRpcBusClient.class, "requests", "ack", "total"));
    private static final Histogram requestsResponseHistogram = metrics.histogram(MetricRegistry.name(DefaultRpcBusClient.class, "requests", "response", "total"));
    private static final Counter errorCounter = metrics.counter(MetricRegistry.name(DefaultRpcBusClient.class, "error"));

    private final BusFactory busFactory;
    private final Lock sessionLock = new ReentrantLock();
    private Session currentSession;
    private boolean closed;
    private final String name; // for debug

    private final class Statistics {
        Histogram requestsAckHistogramLocal;
        Histogram requestsResponseHistogramLocal;

        void updateAck(long millis) {
            requestsResponseHistogramLocal.update(millis);
            requestsResponseHistogram.update(millis);
        }

        void updateResponse(long millis) {
            requestsResponseHistogramLocal.update(millis);
            requestsResponseHistogram.update(millis);
        }
    }

    private final Statistics stats;

    /**
     * Предотвращает дальнейшее использование session
     */
    private void discardSession(Session session) {
        sessionLock.lock();
        try {
            if (currentSession == session) {
                currentSession = null;
            }
        } finally {
            sessionLock.unlock();
        }
    }

    /**
     * Session обрабатывает жизненный цикл работы с bus соединением и запросами через него
     */
    private class Session implements BusListener {
        private final Bus bus;
        private final ConcurrentHashMap<YtGuid, Request> activeRequests = new ConcurrentHashMap<>();
        private final String sessionName = String.format("Session(%s@%s)", name, Integer.toHexString(hashCode()));

        public Session() {
            bus = busFactory.createBus(this);
        }

        public void start() {
            bus.disconnected().addListener(ready -> {
                discardSession(this);
                failPending(ready.isSuccess() ? new ClosedChannelException() : ready.cause());
            });
        }

        public void stop() {
            bus.close();
        }

        public EventLoop eventLoop() {
            return bus.eventLoop();
        }

        @Override
        public void onMessage(Bus bus, List<byte[]> message) {
            if (message.size() < 1) {
                throw new IllegalStateException("Received an empty message");
            }
            byte[] headerPart = message.get(0);
            RpcMessageType type;
            try {
                type = RpcMessageType.fromValue(ByteBuffer.wrap(headerPart).order(ByteOrder.LITTLE_ENDIAN).getInt());
            } catch (RuntimeException e) {
                throw new IllegalStateException("Failed to read message type", e);
            }
            switch (type) {
                case RESPONSE:
                    TResponseHeader header;
                    try {
                        header = TResponseHeader
                                .parseFrom(CodedInputStream.newInstance(headerPart, 4, headerPart.length - 4));
                    } catch (RuntimeException | IOException e) {
                        throw new IllegalStateException("Failed to parse message header", e);
                    }
                    YtGuid requestId = YtGuid.fromProto(header.getRequestId());
                    Request request = activeRequests.get(requestId);
                    if (request == null) {
                        // Может произойти, если мы отменили запрос, но успели получить ответ
                        logger.debug("Received response to an unknown request {}", requestId);
                        return;
                    }
                    if (header.hasError() && header.getError().getCode() != 0) {
                        request.error(new RpcError(header.getError()));
                        return;
                    }

                    request.response(message.subList(1, message.size()));
                    break;
                default:
                    throw new IllegalStateException("Unexpected " + type + " message in a client connection");
            }
        }

        @Override
        public void onConnect(Bus bus) {
            // nothing to do
        }

        @Override
        public void onDisconnect(Bus bus) {
            // nothing to do
        }

        @Override
        public void onException(Bus bus, Throwable cause) {
            // nothing to do
        }

        private void failPending(Throwable cause) {
            Iterator<Request> it = activeRequests.values().iterator();
            while (it.hasNext()) {
                Request request = it.next();
                try {
                    request.error(cause);
                } catch (Throwable e) {
                    logger.debug("Failed while failing an active request", e);
                }
                it.remove();
            }
        }

        public void register(Request request) {
            activeRequests.put(request.requestId, request);
        }

        public boolean unregister(Request request) {
            return activeRequests.remove(request.requestId, request);
        }

        @Override
        public String toString() {
            return sessionName;
        }
    }

    /**
     * Состояние запроса в системе
     */
    private enum RequestState {
        INITIALIZING,
        SENDING,
        ACKED,
        FINISHED,
    }

    private static class Request implements RpcClientRequestControl {
        private final Lock lock = new ReentrantLock();
        private RequestState state = RequestState.INITIALIZING;
        private final Session session;
        private final RpcClientRequest request;
        private final RpcClientResponseHandler handler;
        private final YtGuid requestId;
        private Instant started;
        private final Statistics stat;

        // Подписка на событие с таймаутом, если он есть
        private ScheduledFuture<?> timeoutFuture;

        public Request(Session session, RpcClientRequest request, RpcClientResponseHandler handler, Statistics stat) {
            this.session = Objects.requireNonNull(session);
            this.request = Objects.requireNonNull(request);
            this.handler = Objects.requireNonNull(handler);
            this.requestId = request.getRequestId();
            this.stat = stat;
        }

        /**
         * Запускает выполнение запроса
         */
        public void start() {
            try {
                lock.lock();
                try {
                    if (state != RequestState.INITIALIZING) {
                        throw new IllegalStateException("Request has been started already");
                    }
                    state = RequestState.SENDING;
                } finally {
                    lock.unlock();
                }

                started = Instant.now();
                Duration timeout = request.getTimeout();
                request.header().setStartTime(RpcUtil.instantToMicros(started));
                List<byte[]> message = request.serialize();

                if (!request.isOneWay()) {
                    // Регистрируем только дву-направленные запросы
                    session.register(this);
                }

                BusDeliveryTracking level =
                        request.requestAck() ? BusDeliveryTracking.FULL : BusDeliveryTracking.SENT;

                logger.debug("({}) starting request `{}`", session, requestId);
                session.bus.send(message, level).whenComplete((ignored, exception) -> {
                    Duration elapsed = Duration.between(started, Instant.now());
                    stat.updateAck(elapsed.toMillis());
                    if (exception != null) {
                        error(exception);
                        logger.debug("({}) request `{}` acked in {} ms with error `{}`", session, requestId, elapsed.toMillis(), exception.toString());
                    } else {
                        ack();
                        logger.debug("({}) request `{}` acked in {} ms", session, requestId, elapsed.toMillis());
                    }
                });

                if (timeout != null) {
                    // Регистрируем таймаут после того как положили запрос в очередь
                    lock.lock();
                    try {
                        if (state != RequestState.FINISHED) {
                            // Запрос ещё не успел завершиться
                            timeoutFuture = session.eventLoop()
                                    .schedule(this::handleTimeout, timeout.toNanos(), TimeUnit.NANOSECONDS);
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            } catch (Throwable e) {
                error(e);
            }
        }

        /**
         * Вызывается для перехода в FINISHED состояние, только под локом
         */
        private void finishLocked() {
            state = RequestState.FINISHED;
            if (timeoutFuture != null) {
                timeoutFuture.cancel(false);
                timeoutFuture = null;
            }
        }

        /**
         * Отправляет bus сообщение об отмене запроса
         */
        public CompletableFuture<Void> sendCancellation() {
            TRequestCancelationHeader.Builder builder = TRequestCancelationHeader.newBuilder();
            TRequestHeaderOrBuilder header = request.header();
            builder.setRequestId(header.getRequestId());
            builder.setService(header.getService());
            builder.setMethod(header.getMethod());
            if (header.hasRealmId()) {
                builder.setRealmId(header.getRealmId());
            }
            return session.bus.send(RpcUtil.createCancelMessage(builder.build()), BusDeliveryTracking.NONE);
        }

        public void handleTimeout() {
            error(new TimeoutException("Request timed out"));
        }

        @Override
        public boolean cancel() {
            lock.lock();
            try {
                if (state == RequestState.INITIALIZING) {
                    // Мы ещё даже не начинали отправлять запрос, просто отменяем его
                    throw new IllegalStateException("Request has not been started");
                }
                if (state == RequestState.FINISHED) {
                    // Обработка запроса уже завершена, его нельзя отменить
                    return false;
                }
                finishLocked();
            } finally {
                lock.unlock();
            }
            try {
                // Вызываем обработчик onError, сигнализируя завершение обработки
                handler.onError(new CancellationException());
            } finally {
                if (session.unregister(this)) {
                    // Отправляем сообщение на сервер, но только если пользователь ещё не успел
                    // сделать повторный запрос с таким же requestId. На самом деле здесь есть
                    // небольшой race, но в C++ клиенте почему-то сделано примерно так же.
                    sendCancellation();
                }
            }
            return true;
        }

        /**
         * Вызывается при поступлении подтверждения через bus
         */
        public void ack() {
            lock.lock();
            try {
                if (state != RequestState.SENDING) {
                    return;
                }
                if (request.isOneWay()) {
                    finishLocked();
                } else {
                    state = RequestState.ACKED;
                }
            } finally {
                lock.unlock();
            }
            try {
                handler.onAcknowledgement();
            } catch (Throwable e) {
                error(e);
            }
        }

        /**
         * Вызывается при каких-либо ошибках в обработке
         */
        public void error(Throwable cause) {
            errorCounter.inc();
            lock.lock();
            try {
                if (state == RequestState.FINISHED) {
                    // Обработка запроса уже завершена
                    return;
                }
                finishLocked();
            } finally {
                lock.unlock();
            }
            try {
                handler.onError(cause);
            } finally {
                session.unregister(this);
            }
        }

        /**
         * Вызывается при получении ответа за запрос
         */
        public void response(List<byte[]> attachments) {
            Duration elapsed = Duration.between(started, Instant.now());
            stat.updateResponse(elapsed.toMillis());
            logger.debug("({}) request `{}` finished in {} ms", session, requestId, elapsed.toMillis());

            lock.lock();
            try {
                if (state == RequestState.INITIALIZING) {
                    // Мы получили ответ до того, как приаттачили сессию
                    // Этого не может произойти, проверка просто на всякий случай
                    logger.error("Received response to {} before sending the request", requestId);
                    return;
                }
                if (state == RequestState.FINISHED) {
                    // Обработка запроса уже завершена
                    return;
                }
                finishLocked();
            } finally {
                lock.unlock();
            }
            try {
                try {
                    handler.onResponse(attachments);
                } catch (Throwable e) {
                    handler.onError(e);
                }
            } finally {
                session.unregister(this);
            }
        }
    }

    public DefaultRpcBusClient(BusFactory busFactory) {
        this.busFactory = Objects.requireNonNull(busFactory);
        this.name = String.format("%s@%d", busFactory.destinationName(), System.identityHashCode(this));


        this.stats = new Statistics();
        stats.requestsAckHistogramLocal = metrics.histogram(MetricRegistry.name(DefaultRpcBusClient.class, "requests", "ack", destinationName()));
        stats.requestsResponseHistogramLocal = metrics.histogram(MetricRegistry.name(DefaultRpcBusClient.class, "requests", "response", destinationName()));
    }

    public String destinationName() {
        return busFactory.destinationName();
    }

    @Override
    public String toString() {
        return this.name;
    }

    private Session getSession() {
        sessionLock.lock();
        try {
            if (closed) {
                // Клиент закрыт, сразу фейлим открытие сессии
                throw new IllegalStateException("Client is closed");
            }
            Session session = currentSession;
            if (session == null) {
                session = new Session();
                currentSession = session;
                currentSession.start();
            }
            return session;
        } finally {
            sessionLock.unlock();
        }
    }

    @Override
    public void close() {
        sessionLock.lock();
        try {
            closed = true;
            if (currentSession != null) {
                currentSession.stop();
                currentSession = null;
            }
        } finally {
            sessionLock.unlock();
        }
    }

    @Override
    public RpcClientRequestControl send(RpcClientRequest request, RpcClientResponseHandler handler) {
        Request pendingRequest = new Request(getSession(), request, handler, stats);
        pendingRequest.start();
        return pendingRequest;
    }
}
