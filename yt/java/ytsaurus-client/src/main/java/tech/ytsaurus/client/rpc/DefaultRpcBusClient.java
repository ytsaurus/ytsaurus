package tech.ytsaurus.client.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
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
import tech.ytsaurus.client.bus.Bus;
import tech.ytsaurus.client.bus.BusConnector;
import tech.ytsaurus.client.bus.BusDeliveryTracking;
import tech.ytsaurus.client.bus.BusListener;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpc.TRequestCancelationHeader;
import tech.ytsaurus.rpc.TRequestHeader;
import tech.ytsaurus.rpc.TResponseHeader;
import tech.ytsaurus.rpc.TStreamingFeedbackHeader;
import tech.ytsaurus.rpc.TStreamingParameters;
import tech.ytsaurus.rpc.TStreamingPayloadHeader;

/**
 * Базовая реализация rpc клиента поверх bus
 */
public class DefaultRpcBusClient implements RpcClient {
    private static final Logger logger = LoggerFactory.getLogger(DefaultRpcBusClient.class);

    private final BusConnector busConnector;
    private final SocketAddress address;
    private final String addressString;
    private final Lock sessionLock = new ReentrantLock();
    private Session currentSession;
    private boolean closed;

    // TODO: we should remove destinationName and name and use only addressString
    private final String destinationName; // for debug
    private final String name; // output in user log
    private final DefaultRpcBusClientMetricsHolder metricsHolder = new DefaultRpcBusClientMetricsHolderImpl();
    private final AtomicInteger referenceCounter = new AtomicInteger(1);

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

    public DefaultRpcBusClient(BusConnector busConnector, InetSocketAddress address) {
        this(busConnector, address, address.getHostName());
    }

    public DefaultRpcBusClient(BusConnector busConnector, InetSocketAddress address, String destinationName) {
        this(busConnector, address, address.getHostString() + ":" + address.getPort(), destinationName);
    }

    public DefaultRpcBusClient(BusConnector busConnector, SocketAddress address, String destinationName) {
        this(busConnector, address, address.toString(), destinationName);
    }

    public DefaultRpcBusClient(BusConnector busConnector, SocketAddress address,
                               String addressString, String destinationName) {
        this.busConnector = Objects.requireNonNull(busConnector);
        this.address = Objects.requireNonNull(address);
        this.addressString = addressString;
        this.destinationName = destinationName;
        this.name = String.format("%s@%d", destinationName, System.identityHashCode(this));
        this.stats = new Statistics(destinationName());
    }

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
        private final String sessionName = String.format(
                "Session(%s@%s)",
                addressString,
                Integer.toHexString(hashCode())
        );

        Session() {
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

                case HANDSHAKE: {
                    // Ignoring handshakes.
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
        INITIALIZING(0),
        SENDING(1),
        ACKED(2),
        FINISHED(3);

        final int step;

        RequestState(int step) {
            this.step = step;
        }
    }

    private abstract static class RequestBase implements RpcClientRequestControl {
        protected final Lock lock = new ReentrantLock();
        protected RequestState state = RequestState.INITIALIZING;
        protected final RpcClient sender;
        protected final Session session;

        protected final RpcRequest<?> rpcRequest;
        protected final TRequestHeader.Builder requestHeader;

        protected final GUID requestId;
        protected Instant started;
        protected final Statistics stat;
        protected final RpcOptions options;
        private final String description;

        // Подписка на событие с таймаутом, если он есть
        protected ScheduledFuture<?> timeoutFuture;
        private ScheduledFuture<?> ackTimeoutFuture;

        RequestBase(RpcClient sender, Session session, RpcRequest<?> rpcRequest, RpcOptions options, Statistics stat) {
            this.sender = Objects.requireNonNull(sender);
            this.session = Objects.requireNonNull(session);
            Objects.requireNonNull(rpcRequest);

            this.rpcRequest = Objects.requireNonNull(rpcRequest);
            this.requestHeader = rpcRequest.header.toBuilder();

            this.requestId = RpcUtil.fromProto(rpcRequest.header.getRequestId());
            this.stat = stat;
            this.options = Objects.requireNonNull(options);
            this.description = String.format(
                    "%s/%s/%s",
                    requestHeader.getService(),
                    requestHeader.getMethod(),
                    requestId
            );
        }

        @Override
        public String toString() {
            return description;
        }

        public void response(TResponseHeader header, List<byte[]> attachments) {
            Duration elapsed = Duration.between(started, Instant.now());
            stat.updateResponse(elapsed.toMillis());
            logger.debug("Request `{}` finished in {} ms Session: {}", this, elapsed.toMillis(), session);

            lock.lock();
            try {
                if (state == RequestState.INITIALIZING) {
                    // Мы получили ответ до того, как приаттачили сессию
                    // Этого не может произойти, проверка просто на всякий случай
                    logger.error("Received response to {} before sending the request", this);
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
        }

        public void streamingPayload(TStreamingPayloadHeader header, List<byte[]> attachments) {
            throw new IllegalArgumentException();
        }

        public void streamingFeedback(TStreamingFeedbackHeader header, List<byte[]> attachments) {
            throw new IllegalArgumentException();
        }

        abstract void handleError(Throwable cause);

        void handleAcknowledgement() {
            logger.trace("Ack {}", requestId);
            lock.lock();
            try {
                if (ackTimeoutFuture != null) {
                    ackTimeoutFuture.cancel(true);
                    ackTimeoutFuture = null;
                }
            } finally {
                lock.unlock();
            }
        }

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
                requestHeader.setStartTime(RpcUtil.instantToMicros(started));

                logger.debug("Sending request `{}` Session: {}", this, session);
                session.register(this);

                BusDeliveryTracking level =
                        options.getDefaultRequestAck() ? BusDeliveryTracking.FULL : BusDeliveryTracking.SENT;

                TRequestHeader builtRequestHeader = requestHeader.build();

                RpcRequestsTestingController rpcRequestsTestingController =
                        options.getTestingOptions().getRpcRequestsTestingController();

                if (rpcRequestsTestingController != null) {
                    rpcRequestsTestingController.addRequest(builtRequestHeader, rpcRequest.body);
                }

                final List<byte[]> message = rpcRequest.serialize(builtRequestHeader);
                session.bus.send(message, level).whenComplete((ignored, exception) -> {
                    Duration elapsed = Duration.between(started, Instant.now());
                    stat.updateAck(elapsed.toMillis());
                    if (exception != null) {
                        error(exception);
                        logger.debug("({}) request `{}` acked in {} ms with error `{}`",
                                session,
                                this,
                                elapsed.toMillis(),
                                exception.toString());
                    } else {
                        ack();
                        logger.trace("Request `{}` acked in {} ms",
                                this,
                                elapsed.toMillis());
                    }
                });

                Duration timeout = RpcRequest.getTimeout(requestHeader);
                Duration acknowledgementTimeout = options.getAcknowledgementTimeout().orElse(null);
                // Регистрируем таймаут после того как положили запрос в очередь
                lock.lock();
                try {
                    if (timeout != null && state != RequestState.FINISHED) {
                        // Запрос ещё не успел завершиться
                        timeoutFuture = session.eventLoop()
                                .schedule(this::handleTimeout, timeout.toNanos(), TimeUnit.NANOSECONDS);
                    }

                    if (acknowledgementTimeout != null
                            && options.getDefaultRequestAck()
                            && state.step < RequestState.ACKED.step
                    ) {
                        ackTimeoutFuture = session.eventLoop().schedule(
                                this::onAcknowledgementTimeout,
                                acknowledgementTimeout.toNanos(), TimeUnit.NANOSECONDS);
                    }
                } finally {
                    lock.unlock();
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
            if (ackTimeoutFuture != null) {
                ackTimeoutFuture.cancel(false);
                ackTimeoutFuture = null;
            }
        }

        /**
         * Отправляет bus сообщение об отмене запроса
         */
        public CompletableFuture<Void> sendCancellation() {
            TRequestCancelationHeader.Builder builder = TRequestCancelationHeader.newBuilder();
            builder.setRequestId(requestHeader.getRequestId());
            builder.setService(requestHeader.getService());
            builder.setMethod(requestHeader.getMethod());
            if (requestHeader.hasRealmId()) {
                builder.setRealmId(requestHeader.getRealmId());
            }
            logger.debug("Canceling request {}", this);
            return session.bus.send(RpcUtil.createCancelMessage(builder.build()), BusDeliveryTracking.NONE);
        }

        public void handleTimeout() {
            timeout(new TimeoutException("Request timed out"));
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
                handleAcknowledgement();
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

        private void timeout(TimeoutException error) {
            logger.warn("{}; RequestId: {}", error.toString(), requestId);

            sendCancellation(); // NB. YT-11418
            error(error);
        }

        private void onAcknowledgementTimeout() {
            lock.lock();
            try {
                if (state.step >= RequestState.ACKED.step) {
                    return;
                }
            } finally {
                lock.unlock();
            }
            String message = String.format("Request acknowledgement timed out; requestId: %s; proxy: %s",
                    requestId,
                    sender.getAddressString());
            timeout(new AcknowledgementTimeoutException(message));
        }
    }

    private static class Request extends RequestBase {
        protected final RpcClientResponseHandler handler;

        Request(
                RpcClient sender,
                Session session,
                RpcRequest<?> request,
                RpcClientResponseHandler handler,
                RpcOptions options,
                Statistics stat
        ) {
            super(sender, session, request, options, stat);

            this.handler = Objects.requireNonNull(handler);
        }

        public void handleError(Throwable error) {
            handler.onError(error);
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
            super.response(header, attachments);
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

    private static class StreamingRequest extends RequestBase implements RpcClientStreamControl {
        final RpcStreamConsumer consumer;
        final AtomicInteger sequenceNumber = new AtomicInteger(0);
        Duration readTimeout;
        Duration writeTimeout;

        ScheduledFuture<?> readTimeoutFuture = null;
        ScheduledFuture<?> writeTimeoutFuture = null;

        StreamingRequest(
                RpcClient sender,
                Session session,
                RpcRequest<?> request,
                RpcStreamConsumer consumer,
                RpcOptions options,
                Statistics stat
        ) {
            super(sender, session, request, options, stat);
            this.consumer = consumer;
            this.readTimeout = options.getStreamingReadTimeout().orElse(null);
            this.writeTimeout = options.getStreamingWriteTimeout().orElse(null);
            this.resetWriteTimeout();
            this.resetReadTimeout();
            setStreamingOptions();
        }

        @Override
        public void start() {
            super.start();
            consumer.onStartStream(this);
        }

        private void setStreamingOptions() {
            requestHeader.clearTimeout();

            TStreamingParameters.Builder builder = TStreamingParameters.newBuilder();

            if (readTimeout != null) {
                builder.setReadTimeout(RpcUtil.durationToMicros(readTimeout));
            }
            if (writeTimeout != null) {
                builder.setWriteTimeout(RpcUtil.durationToMicros(writeTimeout));
            }
            builder.setWindowSize(options.getStreamingWindowSize());
            requestHeader.setServerAttachmentsStreamingParameters(builder);
        }

        @Override
        void handleError(Throwable cause) {
            logger.info("Error in RPC protocol: `{}`", cause.getMessage(), cause);
            lock.lock();
            try {
                consumer.onError(cause);
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
                consumer.onCancel(cancel);
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
        public Compression getExpectedPayloadCompression() {
            if (requestHeader.hasRequestCodec()) {
                return Compression.fromValue(requestHeader.getRequestCodec());
            } else {
                return Compression.None;
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
                    logger.error("Received response to {} before sending the request", this);
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
                    logger.error("Received response to {} before sending the request", this);
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
        protected void finishLocked() {
            clearReadTimeout();
            clearWriteTimeout();
            state = RequestState.FINISHED;
        }

        @Override
        public void response(TResponseHeader header, List<byte[]> attachments) {
            super.response(header, attachments);
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
        public CompletableFuture<Void> feedback(long offset) {
            TStreamingFeedbackHeader.Builder builder = TStreamingFeedbackHeader.newBuilder();
            builder.setRequestId(requestHeader.getRequestId());
            builder.setService(requestHeader.getService());
            builder.setMethod(requestHeader.getMethod());
            if (requestHeader.hasRealmId()) {
                builder.setRealmId(requestHeader.getRealmId());
            }
            builder.setReadPosition(offset);
            return session.bus.send(
                    Collections.singletonList(
                            RpcUtil.createMessageHeader(RpcMessageType.STREAMING_FEEDBACK, builder.build())
                    ),
                    BusDeliveryTracking.NONE
            );
        }

        @Override
        public CompletableFuture<Void> sendEof() {
            TStreamingPayloadHeader.Builder builder = TStreamingPayloadHeader.newBuilder();
            builder.setRequestId(requestHeader.getRequestId());
            builder.setService(requestHeader.getService());
            builder.setMethod(requestHeader.getMethod());
            builder.setSequenceNumber(sequenceNumber.getAndIncrement());
            if (requestHeader.hasRealmId()) {
                builder.setRealmId(requestHeader.getRealmId());
            }
            return session.bus.send(RpcUtil.createEofMessage(builder.build()), BusDeliveryTracking.NONE)
                    .thenAccept((unused) -> {
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
            builder.setRequestId(requestHeader.getRequestId());
            builder.setService(requestHeader.getService());
            builder.setMethod(requestHeader.getMethod());
            builder.setSequenceNumber(sequenceNumber.getAndIncrement());
            builder.setCodec(requestHeader.getRequestCodec());
            if (requestHeader.hasRealmId()) {
                builder.setRealmId(requestHeader.getRealmId());
            }

            return RpcUtil.createMessageHeader(RpcMessageType.STREAMING_PAYLOAD, builder.build());
        }

        @Override
        public CompletableFuture<Void> sendPayload(List<byte[]> attachments) {
            List<byte[]> message = new ArrayList<>(1 + attachments.size());
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

        @Override
        public String getRpcProxyAddress() {
            return sender.getAddressString();
        }
    }

    public String destinationName() {
        return this.destinationName;
    }

    @Override
    public String getAddressString() {
        return addressString;
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
    public void ref() {
        int oldValue = referenceCounter.getAndIncrement();
        if (oldValue <= 0) {
            throw new IllegalStateException("Trying to ref dead object");
        }
    }

    @Override
    public void unref() {
        int newValue = referenceCounter.decrementAndGet();
        if (newValue < 0) {
            throw new IllegalStateException("Trying to unref dead object");
        }
        if (newValue == 0) {
            close();
        }
    }

    @Override
    public void close() {
        logger.debug("Closing RpcClient: {}", this);
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
    public RpcClientRequestControl send(
            RpcClient sender,
            RpcRequest<?> request,
            RpcClientResponseHandler handler,
            RpcOptions options
    ) {
        RequestBase pendingRequest = new Request(sender, getSession(), request, handler, options, stats);
        pendingRequest.start();
        return pendingRequest;
    }

    @Override
    public RpcClientStreamControl startStream(
            RpcClient sender,
            RpcRequest<?> request,
            RpcStreamConsumer consumer,
            RpcOptions options
    ) {
        StreamingRequest pendingRequest = new StreamingRequest(sender, getSession(), request, consumer, options, stats);
        pendingRequest.start();
        return pendingRequest;
    }

    @Override
    public ScheduledExecutorService executor() {
        return getSession().eventLoop();
    }
}
