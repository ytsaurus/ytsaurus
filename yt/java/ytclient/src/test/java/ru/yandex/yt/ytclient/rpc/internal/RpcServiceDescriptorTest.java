package ru.yandex.yt.ytclient.rpc.internal;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.MessageLite;
import org.junit.Test;

import ru.yandex.yt.rpc.TReqDiscover;
import ru.yandex.yt.rpc.TRspDiscover;
import ru.yandex.yt.rpcproxy.TReqGetNode;
import ru.yandex.yt.rpcproxy.TRspGetNode;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class RpcServiceDescriptorTest {
    private <T extends MessageLite.Builder> void verifyDescriptorRequest(RpcServiceMethodDescriptor m,
            Class<T> bodyClass)
    {
        assertThat(m.getRequestBodyType(), is(equalTo(bodyClass)));
        Object body = m.getRequestBodyCreator().get();
        assertThat(body, is(instanceOf(bodyClass)));
    }

    private <T extends MessageLite> void verifyDescriptorResponse(RpcServiceMethodDescriptor m, Class<T> messageClass,
            T sampleMessage) throws IOException
    {
        assertThat(m.getResponseBodyType(), is(equalTo(messageClass)));
        byte[] data = sampleMessage.toByteArray();
        Object message = m.getResponseBodyParser().parseFrom(data);
        assertThat(message, is(instanceOf(messageClass)));
        assertThat(message, is(equalTo(sampleMessage)));
    }

    public interface ResponseMethod {
        RpcClientRequestBuilder<TReqDiscover.Builder, RpcClientResponse<TRspDiscover>> discover();
    }

    @Test
    public void responseMethod() throws IOException {
        RpcServiceDescriptor d = RpcServiceDescriptor.forInterface(ResponseMethod.class);
        assertThat(d.getServiceName(), is("ResponseMethod"));
        assertThat(d.getMethodList(), hasSize(1));
        RpcServiceMethodDescriptor m = d.getMethodList().get(0);
        assertThat(m.getMethodName(), is("Discover"));
        verifyDescriptorRequest(m, TReqDiscover.Builder.class);
        verifyDescriptorResponse(m, TRspDiscover.class, TRspDiscover.newBuilder()
                .addSuggestedAddresses("hello")
                .addSuggestedAddresses("world")
                .setUp(true)
                .build());
    }

    public interface WithDefaultMethod {
        RpcClientRequestBuilder<TReqDiscover.Builder, RpcClientResponse<TRspDiscover>> discover();

        // default methods should not be allowed
        default CompletableFuture<TRspDiscover> discover(TReqDiscover request) {
            return null;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void withDefaultMethod() {
        RpcServiceDescriptor d = RpcServiceDescriptor.forInterface(WithDefaultMethod.class);
        assertThat(d.getMethodList(), hasSize(1));
    }

    public interface CommonServiceMethods {
        RpcClientRequestBuilder<TReqDiscover.Builder, RpcClientResponse<TRspDiscover>> discover();
    }

    public interface SomeService extends CommonServiceMethods {
        RpcClientRequestBuilder<TReqGetNode.Builder, RpcClientResponse<TRspGetNode>> getNode();
    }

    @Test
    public void inheritedMethods() {
        RpcServiceDescriptor d = RpcServiceDescriptor.forInterface(SomeService.class);
        assertThat(d.getServiceName(), is("SomeService"));
        assertThat(d.getMethodList(), hasSize(2));
    }

    public interface WithStaticMethod {
        RpcClientRequestBuilder<TReqDiscover.Builder, RpcClientResponse<TRspDiscover>> discover();

        static void doSomethingCool() {
            // does something cool
        }
    }

    @Test
    public void withStaticMethod() {
        RpcServiceDescriptor d = RpcServiceDescriptor.forInterface(WithStaticMethod.class);
        assertThat(d.getMethodList(), hasSize(1));
    }
}
