package ru.yandex.yt.ytclient.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.protobuf.CodedInputStream;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.yt.rpc.TRequestCancelationHeader;
import ru.yandex.yt.rpc.TRequestHeaderOrBuilder;
import ru.yandex.yt.rpc.TResponseHeader;
import ru.yandex.yt.rpc.TStreamingFeedbackHeader;
import ru.yandex.yt.rpc.TStreamingParameters;
import ru.yandex.yt.rpc.TStreamingPayloadHeader;
import ru.yandex.yt.ytclient.bus.Bus;
import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.bus.BusDeliveryTracking;
import ru.yandex.yt.ytclient.bus.BusListener;
import ru.yandex.yt.ytclient.rpc.internal.Compression;
import ru.yandex.yt.ytclient.rpc.metrics.DefaultRpcBusClientMetricsHolder;
import ru.yandex.yt.ytclient.rpc.metrics.DefaultRpcBusClientMetricsHolderImpl;

/**
 * Базовая реализация rpc клиента поверх bus
 */
public class DefaultRpcBusClient implements RpcClient {
    private static final Logger logger = LoggerFactory.getLogger(DefaultRpcBusClient.class);

    private final BusConnector busConnector;
    private final InetSocketAddress address;
    private final Lock sessionLock = new ReentrantLock();
    private Session currentSession;
    private boolean closed;
    private final String destinationName; // for debug
    private final String name; // output in user log

    private final DefaultRpcBusClientMetricsHolder metricsHolder;

    private final class Statistics {
        private final String name;

        Statistics(String name) {
            this.name = name;
        }

        void updateAck(long millis) {
            metricsHolder.updateAck(name, millis);
        }

        void updateResponse(long millis) {
            metricsHolder.updateResponse(name, millis);
        }

        void incError() {
            metricsHolder.incError();
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
        private final ConcurrentHashMap<GUID, RequestBase> activeRequests = new ConcurrentHashMap<>();
        private final String sessionName = String.format("Session(%s@%s)", name, Integer.toHexString(hashCode()));

        public Session() {
            bus = busConnector.connect(address, this);
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
                case RESPONSE: {
                    TResponseHeader header;
                    try {
                        header = TResponseHeader
                                .parseFrom(CodedInputStream.newInstance(headerPart, 4, headerPart.length - 4));
                    } catch (RuntimeException | IOException e) {
                        throw new IllegalStateException("Failed to parse message header", e);
                    }

                    GUID requestId = RpcUtil.fromProto(header.getRequestId());

                    RequestBase request = activeRequests.get(requestId);
                    if (request == null) {
                        // Может произойти, если мы отменили запрос, но успели получить ответ
                        logger.debug("Received response to an unknown request {}", requestId);
                        return;
                    }

                    if (header.hasError() && header.getError().getCode() != 0) {
                        request.error(new RpcError(header.getError()));
                        return;
                    }

                    request.response(header, message.subList(1, message.size()));

                    break;
                }

                case STREAMING_PAYLOAD: {

                    TStreamingPayloadHeader header;
                    try {
                        header = TStreamingPayloadHeader
                                .parseFrom(CodedInputStream.newInstance(headerPart, 4, headerPart.length - 4));
                    } catch (RuntimeException | IOException e) {
                        throw new IllegalStateException("Failed to parse message header", e);
                    }

                    GUID requestId = RpcUtil.fromProto(header.getRequestId());

                    RequestBase request = activeRequests.get(requestId);
                    if (request == null) {
                        // Может произойти, если мы отменили запрос, но успели получить ответ
                        logger.debug("Received response to an unknown request {}", requestId);
                        return;
                    }

                    request.streamingPayload(header, message.subList(1, message.size()));

                    break;
                }

                case STREAMING_FEEDBACK: {
                    TStreamingFeedbackHeader header;
                    try {
                        header = TStreamingFeedbackHeader
                                .parseFrom(CodedInputStream.newInstance(headerPart, 4, headerPart.length - 4));
                    } catch (RuntimeException | IOException e) {
                        throw new IllegalStateException("Failed to parse message header", e);
                    }

                    GUID requestId = RpcUtil.fromProto(header.getRequestId());

                    RequestBase request = activeRequests.get(requestId);
                    if (request == null) {
                        // Может произойти, если мы отменили запрос, но успели получить ответ
                        logger.debug("Received response to an unknown request {}", requestId);
                        return;
                    }

                    request.streamingFeedback(header, message.subList(1, message.size()));

                    break;
                }
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
            Iterator<RequestBase> it = activeRequests.values().iterator();
            while (it.hasNext()) {
                RequestBase request = it.next();
                try {
                    request.error(cause);
                } catch (Throwable e) {
                    logger.debug("Failed while failing an active request", e);
                }
                it.remove();
            }
        }

        public void register(RequestBase request) {
            activeRequests.put(request.requestId, request);
        }

        public boolean unregister(RequestBase request) {
            logger.trace("Unregister request {}", request.requestId);
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

    private static abstract class RequestBase implements RpcClientRequestControl {
        protected final Lock lock = new ReentrantLock();
        protected RequestState state = RequestState.INITIALIZING;
        protected final RpcClient sender;
        protected final Session session;
        protected final RpcClientRequest request;
        protected final GUID requestId;
        protected Instant started;
        protected final Statistics stat;

        // Подписка на событие с таймаутом, если он есть
        protected ScheduledFuture<?> timeoutFuture;

        RequestBase(RpcClient sender, Session session, RpcClientRequest request, Statistics stat) {
            this.sender = Objects.requireNonNull(sender);
            this.session = Objects.requireNonNull(session);
            this.request = Objects.requireNonNull(request);
            this.requestId = request.getRequestId();
            this.stat = stat;
        }

        public void response(TResponseHeader header, List<byte[]> attachments) {
            throw new IllegalArgumentException();
        }

        public void streamingPayload(TStreamingPayloadHeader header, List<byte[]> attachments) {
            throw new IllegalArgumentException();
        }

        public void streamingFeedback(TStreamingFeedbackHeader header, List<byte[]> attachments) {
            throw new IllegalArgumentException();
        }

        abstract void handleError(Throwable cause);

        abstract void handleAcknowledgement(RpcClient sender);

        abstract void handleCancellation(CancellationException cancel);

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

                session.register(this);

                BusDeliveryTracking level =
                        request.requestAck() ? BusDeliveryTracking.FULL : BusDeliveryTracking.SENT;

                logger.debug("({}) starting request `{}`", session, request);
                session.bus.send(message, level).whenComplete((ignored, exception) -> {
                    Duration elapsed = Duration.between(started, Instant.now());
                    stat.updateAck(elapsed.toMillis());
                    if (exception != null) {
                        error(exception);
                        logger.debug("({}) request `{}` acked in {} ms with error `{}`", session, request, elapsed.toMillis(), exception.toString());
                    } else {
                        ack();
                        logger.trace("({}) request `{}` acked in {} ms", session, request, elapsed.toMillis());
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
        protected void finishLocked() {
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
            logger.warn("Request timed out; RequestId: {}", requestId);

            sendCancellation(); // NB. YT-11418
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
                // Вызываем обработчик onCancel, сигнализируя завершение обработки
                handleCancellation(new CancellationException());
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
                state = RequestState.ACKED;
            } finally {
                lock.unlock();
            }
            try {
                handleAcknowledgement(sender);
            } catch (Throwable e) {
                error(e);
            }
        }

        /**
         * Вызывается при каких-либо ошибках в обработке
         */
        public void error(Throwable cause) {
            stat.incError();
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
                handleError(cause);
            } finally {
                session.unregister(this);
            }
        }
    }

    private static class Request extends RequestBase {
        protected final RpcClientResponseHandler handler;

        public Request(RpcClient sender, Session session, RpcClientRequest request, RpcClientResponseHandler handler, Statistics stat) {
            super(sender, session, request, stat);

            this.handler = Objects.requireNonNull(handler);
        }

        public void handleError(Throwable error) {
            handler.onError(error);
        }

        @Override
        public void handleAcknowledgement(RpcClient sender) {
        }

        @Override
        void handleCancellation(CancellationException cancel) {
            handler.onCancel(cancel);
        }

        /**
         * Вызывается при получении ответа за запрос
         */
        @Override
        public void response(TResponseHeader header, List<byte[]> attachments) {
            Duration elapsed = Duration.between(started, Instant.now());
            stat.updateResponse(elapsed.toMillis());
            logger.debug("({}) request `{}` finished in {} ms", session, request, elapsed.toMillis());

            lock.lock();
            try {
                if (state == RequestState.INITIALIZING) {
                    // Мы получили ответ до того, как приаттачили сессию
                    // Этого не может произойти, проверка просто на всякий случай
                    logger.error("Received response to {} before sending the request", request);
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
                    handler.onResponse(sender, header, attachments);
                } catch (Throwable e) {
                    handler.onError(e);
                }
            } finally {
                session.unregister(this);
            }
        }
    }

    private static class StashedMessage {
        TStreamingFeedbackHeader feedbackHeader = null;
        TStreamingPayloadHeader payloadHeader = null;
        TResponseHeader responseHeader = null;
        List<byte[]> attachments = null;
        Throwable cause = null;
        CancellationException cancel = null;
    }

    private static class Stash implements RpcStreamConsumer {
        ArrayList<StashedMessage> stashedMessages = new ArrayList<>();
        int nextStashedMessageIndex = 0;

        @Override
        public void onFeedback(RpcClient unused, TStreamingFeedbackHeader header, List<byte[]> attachments) {
            StashedMessage message = new StashedMessage();
            message.feedbackHeader = header;
            message.attachments = attachments;
            stashedMessages.add(message);
        }

        @Override
        public void onPayload(RpcClient unused, TStreamingPayloadHeader header, List<byte[]> attachments) {
            StashedMessage message = new StashedMessage();
            message.payloadHeader = header;
            message.attachments = attachments;
            stashedMessages.add(message);
        }

        @Override
        public void onResponse(RpcClient unused, TResponseHeader header, List<byte[]> attachments) {
            StashedMessage message = new StashedMessage();
            message.responseHeader = header;
            message.attachments = attachments;
            stashedMessages.add(message);
        }

        @Override
        public void onError(RpcClient unused, Throwable cause) {
            StashedMessage message = new StashedMessage();
            message.cause = cause;
            stashedMessages.add(message);
        }

        @Override
        public void onCancel(RpcClient sender, CancellationException cancel) {
            StashedMessage message = new StashedMessage();
            message.cancel = cancel;
            stashedMessages.add(message);
        }

        void unstash(RpcClient sender, RpcStreamConsumer consumer) {
            while (nextStashedMessageIndex < stashedMessages.size()) {
                StashedMessage message = stashedMessages.get(nextStashedMessageIndex++);

                if (message.feedbackHeader != null) {
                    consumer.onFeedback(sender, message.feedbackHeader, message.attachments);
                } else if (message.payloadHeader != null) {
                    consumer.onPayload(sender, message.payloadHeader, message.attachments);
                } else if (message.responseHeader != null) {
                    consumer.onResponse(sender, message.responseHeader, message.attachments);
                } else if (message.cause != null) {
                    consumer.onError(sender, message.cause);
                } else if (message.cancel != null) {
                    consumer.onCancel(sender, message.cancel);
                }
            }
        }
    }

    private static class StreamingRequest extends RequestBase implements RpcClientStreamControl {
        RpcStreamConsumer consumer = new Stash();
        final AtomicInteger sequenceNumber = new AtomicInteger(0);
        Duration readTimeout;
        Duration writeTimeout;
        final RpcOptions options;

        ScheduledFuture<?> readTimeoutFuture = null;
        ScheduledFuture<?> writeTimeoutFuture = null;

        StreamingRequest(RpcClient sender, Session session, RpcClientRequest request, Statistics stat) {
            super(sender, session, request, stat);
            this.options = request.getOptions();
            this.readTimeout = options.getStreamingReadTimeout();
            this.writeTimeout = options.getStreamingWriteTimeout();
            this.resetWriteTimeout();
            this.resetReadTimeout();
            setStreamingOptions();
        }

        private void setStreamingOptions() {
            request.header().clearTimeout();

            TStreamingParameters.Builder builder = TStreamingParameters.newBuilder();

            if (readTimeout != null) {
                builder.setReadTimeout(RpcUtil.durationToMicros(readTimeout));
            }
            if (writeTimeout != null) {
                builder.setWriteTimeout(RpcUtil.durationToMicros(writeTimeout));
            }
            builder.setWindowSize(options.getStreamingWindowSize());
            request.header().setServerAttachmentsStreamingParameters(builder);
        }

        @Override
        public Compression compression() {
            if (request.header().hasRequestCodec()) {
                return Compression.fromValue(request.header().getRequestCodec());
            } else {
                return Compression.None;
            }
        }

        @Override
        void handleAcknowledgement(RpcClient sender) {
            logger.debug("Ack {}", requestId);
        }

        @Override
        void handleError(Throwable cause) {
            logger.info("Error in RPC protocol: `{}`", cause.getMessage(), cause);
            lock.lock();
            try {
                consumer.onError(sender, cause);
            } catch (Throwable e) {
                logger.error("Error", e);
            } finally {
                lock.unlock();
            }
        }

        @Override
        void handleCancellation(CancellationException cancel) {
            lock.lock();
            try {
                consumer.onCancel(sender, cancel);
            } catch (Throwable e) {
                logger.error("Error", e);
            } finally {
                lock.unlock();
            }
        }

        private void clearReadTimeout() {
            if (readTimeoutFuture != null) {
                readTimeoutFuture.cancel(false);
            }
            readTimeout = null;
        }

        private void clearWriteTimeout() {
            if (writeTimeoutFuture != null) {
                writeTimeoutFuture.cancel(false);
            }
            writeTimeout = null;
        }

        private void resetReadTimeout() {
            if (readTimeout != null) {
                if (readTimeoutFuture != null) {
                    readTimeoutFuture.cancel(false);
                }
                readTimeoutFuture = session.eventLoop()
                        .schedule(this::handleTimeout, readTimeout.toNanos(), TimeUnit.NANOSECONDS);
            }
        }

        private void resetWriteTimeout() {
            if (writeTimeout != null) {
                if (writeTimeoutFuture != null) {
                    writeTimeoutFuture.cancel(false);
                }
                writeTimeoutFuture = session.eventLoop()
                        .schedule(this::handleTimeout, writeTimeout.toNanos(), TimeUnit.NANOSECONDS);
            }
        }

        @Override
        public void streamingPayload(TStreamingPayloadHeader header, List<byte[]> attachments) {
            Duration elapsed = Duration.between(started, Instant.now());
            stat.updateResponse(elapsed.toMillis());

            lock.lock();
            try {
                if (state == RequestState.INITIALIZING) {
                    // Мы получили ответ до того, как приаттачили сессию
                    // Этого не может произойти, проверка просто на всякий случай
                    logger.error("Received response to {} before sending the request", request);
                    return;
                }
                if (state == RequestState.FINISHED) {
                    // Обработка запроса уже завершена
                    return;
                }
            } finally {
                resetWriteTimeout();
                lock.unlock();
            }

            try {
                lock.lock();
                consumer.onPayload(sender, header, attachments);
            } catch (Throwable e) {
                handleError(e);
                session.unregister(this);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void streamingFeedback(TStreamingFeedbackHeader header, List<byte[]> attachments) {
            Duration elapsed = Duration.between(started, Instant.now());
            stat.updateResponse(elapsed.toMillis());

            lock.lock();
            try {
                if (state == RequestState.INITIALIZING) {
                    // Мы получили ответ до того, как приаттачили сессию
                    // Этого не может произойти, проверка просто на всякий случай
                    logger.error("Received response to {} before sending the request", request);
                    return;
                }
                if (state == RequestState.FINISHED) {
                    // Обработка запроса уже завершена
                    return;
                }
            } finally {
                resetReadTimeout();
                lock.unlock();
            }

            try {
                lock.lock();
                consumer.onFeedback(sender, header, attachments);
            } catch (Throwable e) {
                handleError(e);
                session.unregister(this);
            } finally {
                lock.unlock();
            }
        }

        @Override
        protected void finishLocked()
        {
            clearReadTimeout();
            clearWriteTimeout();
            state = RequestState.FINISHED;
        }

        @Override
        public void response(TResponseHeader header, List<byte[]> attachments) {
            Duration elapsed = Duration.between(started, Instant.now());
            stat.updateResponse(elapsed.toMillis());
            logger.debug("({}) request `{}` finished in {} ms", session, request, elapsed.toMillis());

            lock.lock();
            try {
                if (state == RequestState.INITIALIZING) {
                    // Мы получили ответ до того, как приаттачили сессию
                    // Этого не может произойти, проверка просто на всякий случай
                    logger.error("Received response to {} before sending the request", request);
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
                lock.lock();
                consumer.onResponse(sender, header, attachments);
            } catch (Throwable e) {
                handleError(e);
            } finally {
                session.unregister(this);
                lock.unlock();
            }
        }

        @Override
        public void subscribe(RpcStreamConsumer consumer) {
            lock.lock();
            try {
                Stash stash = (Stash) this.consumer;
                this.consumer = consumer;
                // Make sure to fully drain the stash.
                while (true) {
                    try {
                        stash.unstash(sender, this.consumer);
                        break;
                    } catch (Throwable e) {
                        handleError(e);
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public CompletableFuture<Void> feedback(long offset) {
            TStreamingFeedbackHeader.Builder builder = TStreamingFeedbackHeader.newBuilder();
            TRequestHeaderOrBuilder header = request.header();
            builder.setRequestId(header.getRequestId());
            builder.setService(header.getService());
            builder.setMethod(header.getMethod());
            if (header.hasRealmId()) {
                builder.setRealmId(header.getRealmId());
            }
            builder.setReadPosition(offset);
            return session.bus.send(Collections.singletonList(RpcUtil.createMessageHeader(RpcMessageType.STREAMING_FEEDBACK, builder.build())), BusDeliveryTracking.NONE);
        }

        @Override
        public CompletableFuture<Void> sendEof() {
            TStreamingPayloadHeader.Builder builder = TStreamingPayloadHeader.newBuilder();
            TRequestHeaderOrBuilder header = request.header();
            builder.setRequestId(header.getRequestId());
            builder.setService(header.getService());
            builder.setMethod(header.getMethod());
            builder.setSequenceNumber(sequenceNumber.getAndIncrement());
            if (header.hasRealmId()) {
                builder.setRealmId(header.getRealmId());
            }
            return session.bus.send(RpcUtil.createEofMessage(builder.build()), BusDeliveryTracking.NONE).thenAccept((unused) -> {
                lock.lock();
                try {
                    clearReadTimeout();
                } finally {
                    lock.unlock();
                }
            });
        }

        private byte[] preparePayloadHeader() {
            TStreamingPayloadHeader.Builder builder = TStreamingPayloadHeader.newBuilder();
            TRequestHeaderOrBuilder header = request.header();
            builder.setRequestId(header.getRequestId());
            builder.setService(header.getService());
            builder.setMethod(header.getMethod());
            builder.setSequenceNumber(sequenceNumber.getAndIncrement());
            builder.setCodec(request.header().getRequestCodec());
            if (header.hasRealmId()) {
                builder.setRealmId(header.getRealmId());
            }

            return RpcUtil.createMessageHeader(RpcMessageType.STREAMING_PAYLOAD, builder.build());
        }

        @Override
        public CompletableFuture<Void> sendPayload(List<byte[]> attachments)
        {
            List<byte[]> message = new ArrayList<>(1+attachments.size());
            message.add(preparePayloadHeader());
            message.addAll(attachments);

            return session.bus.send(message, BusDeliveryTracking.NONE).thenAccept((unused) -> {
                lock.lock();
                try {
                    resetReadTimeout();
                } finally {
                    lock.unlock();
                }
            });
        }

        private void doConsumerWakeup() {
            try {
                consumer.onWakeup();
            } catch (Throwable ex) {
                error(ex);
            }
        }

        @Override
        public void wakeUp() {
            session.eventLoop().schedule(this::doConsumerWakeup, 0, TimeUnit.MILLISECONDS);
        }
    }

    public DefaultRpcBusClient(BusConnector busFactory, InetSocketAddress address) {
        this(busFactory, address, address.getHostName(), new DefaultRpcBusClientMetricsHolderImpl());
    }

    public DefaultRpcBusClient(BusConnector busFactory, InetSocketAddress address, String destinationName) {
        this(busFactory, address, destinationName, new DefaultRpcBusClientMetricsHolderImpl());
    }

    public DefaultRpcBusClient(BusConnector busConnector, InetSocketAddress address, String destinationName, DefaultRpcBusClientMetricsHolder metricsHolder) {
        this.busConnector = Objects.requireNonNull(busConnector);
        this.address = Objects.requireNonNull(address);
        this.destinationName = destinationName;
        this.name = String.format("%s@%d", destinationName, System.identityHashCode(this));
        this.stats = new Statistics(destinationName());
        this.metricsHolder = metricsHolder;
    }

    public String destinationName() {
        return this.destinationName;
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
    public RpcClientRequestControl send(RpcClient sender, RpcClientRequest request, RpcClientResponseHandler handler) {
        RequestBase pendingRequest = new Request(sender, getSession(), request, handler, stats);
        pendingRequest.start();
        return pendingRequest;
    }

    @Override
    public RpcClientStreamControl startStream(RpcClient sender, RpcClientRequest request) {
        StreamingRequest pendingRequest = new StreamingRequest(sender, getSession(), request, stats);
        pendingRequest.start();
        return pendingRequest;
    }

    @Override
    public ScheduledExecutorService executor()
    {
        return getSession().eventLoop();
    }
}
