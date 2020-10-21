package ru.yandex.yt.ytclient.rpc.internal;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import ru.yandex.yt.rpc.TReqDiscover;
import ru.yandex.yt.rpc.TResponseHeader;
import ru.yandex.yt.rpc.TRspDiscover;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequest;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestControl;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcStreamConsumer;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class RpcServiceClientTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    /**
     * Очень простая реализация клиента всегда дающая фиксированный ответ
     */
    public static class RespondingClient implements RpcClient {
        private final List<byte[]> attachments;

        public RespondingClient(byte[]... attachments) {
            this.attachments = Arrays.asList(attachments);
        }

        @Override
        public void ref() {
        }

        @Override
        public void unref() {
        }

        @Override
        public void close() {
        }

        @Override
        public RpcClientRequestControl send(RpcClient unused, RpcClientRequest request, RpcClientResponseHandler handler) {
            request.serialize();
            handler.onResponse(this, TResponseHeader.newBuilder().build(), attachments);
            return () -> false;
        }

        @Override
        public RpcClientStreamControl startStream(RpcClient sender, RpcClientRequest request, RpcStreamConsumer consumer) {
            return null;
        }

        @Override
        public String destinationName() {
            return "RespondingClient";
        }

        @Override
        public String getAddressString() {
            return null;
        }

        @Override
        public ScheduledExecutorService executor() {
            throw new IllegalArgumentException("unreachable");
        }
    }

    public static class FailingClient implements RpcClient {
        private final Throwable error;

        public FailingClient(Throwable error) {
            this.error = error;
        }

        @Override
        public void ref() {
        }

        @Override
        public void unref() {
        }

        @Override
        public void close() {
        }

        @Override
        public RpcClientRequestControl send(RpcClient unused, RpcClientRequest request, RpcClientResponseHandler handler) {
            request.serialize();
            handler.onError(error);
            return () -> false;
        }

        @Override
        public RpcClientStreamControl startStream(RpcClient sender, RpcClientRequest request, RpcStreamConsumer consumer) {
            return null;
        }

        @Override
        public String destinationName() {
            return "FailingClient";
        }

        @Override
        public String getAddressString() {
            return null;
        }

        @Override
        public ScheduledExecutorService executor() {
            throw new IllegalArgumentException("unreachable");
        }
    }

    public interface MyService {
        RpcClientRequestBuilder<TReqDiscover.Builder, RpcClientResponse<TRspDiscover>> discover();
    }

    public static class MyServiceClient {
        private final RpcClient client;
        private final MyService service;

        public MyServiceClient(RpcClient client) {
            this.client = client;
            this.service = RpcServiceClient.create(MyService.class);
        }

        public TRspDiscover discover(TReqDiscover req) {
            RpcClientRequestBuilder<TReqDiscover.Builder, RpcClientResponse<TRspDiscover>> builder = service.discover();
            assertThat(builder.getService(), is("MyService"));
            assertThat(builder.getMethod(), is("Discover"));
            builder.body().mergeFrom(req);
            return builder.invoke(client).join().body();
        }
    }

    @Test
    public void workingCall() {
        TRspDiscover expectedResponse = TRspDiscover.newBuilder()
                .setUp(true)
                .addSuggestedAddresses("hello")
                .addSuggestedAddresses("world")
                .build();
        RpcClient rpcClient = new RespondingClient(RpcUtil.createMessageBodyWithEnvelope(expectedResponse));
        MyServiceClient serviceClient = new MyServiceClient(rpcClient);
        TRspDiscover response = serviceClient.discover(TReqDiscover.newBuilder().build());
        assertThat(response, is(expectedResponse));
    }

    public static class MyException extends RuntimeException {
        public MyException(String message) {
            super(message);
        }
    }

    @Test
    public void failingCall() throws Throwable {
        RpcClient rpcClient = new FailingClient(new MyException("This is an error"));
        MyServiceClient serviceClient = new MyServiceClient(rpcClient);
        exception.expect(MyException.class);
        exception.expectMessage("This is an error");
        try {
            serviceClient.discover(TReqDiscover.newBuilder().build());
        } catch (CompletionException e) {
            throw e.getCause();
        }
    }

    @Test
    @SuppressWarnings({"unused", "EqualsWithItself"})
    public void standardMethods() {
        RpcClient client = new FailingClient(new MyException("Not used"));
        MyService service = RpcServiceClient.create(MyService.class);
        int h = service.hashCode();
        String s = service.toString();
        assertThat(s, is(not("")));
        boolean e = service.equals(service);
        assertThat(e, is(true));
    }
}
