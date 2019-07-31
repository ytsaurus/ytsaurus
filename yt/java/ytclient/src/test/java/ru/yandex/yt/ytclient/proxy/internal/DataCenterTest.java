package ru.yandex.yt.ytclient.proxy.internal;

import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.Assert;
import org.junit.Test;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequest;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestControl;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;

public class DataCenterTest {

    class FakeRpcClient implements RpcClient {
        private final String name;

        public FakeRpcClient(String name)
        {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }

        @Override
        public void close() {
            // nothing
        }

        @Override
        public RpcClientRequestControl send(RpcClient unused, RpcClientRequest request, RpcClientResponseHandler handler) {
            throw new IllegalArgumentException("unreachable");
        }

        @Override
        public RpcClientStreamControl startStream(RpcClient sender, RpcClientRequest request) {
            throw new IllegalArgumentException("unreachable");
        }

        @Override
        public String destinationName() {
            return "FakeRpcClient";
        }

        @Override
        public ScheduledExecutorService executor() {
            throw new IllegalArgumentException("unreachable");
        }
    }

    @Test
    public void testAddRemoveBackends() {
        BalancingDestination initial [] = new BalancingDestination[0];

        DataCenter dc = new DataCenter("test", initial, -1.0);

        Set<RpcClient> addedProxies = Cf.set(new FakeRpcClient("1"), new FakeRpcClient("2"));
        dc.addProxies(addedProxies);
        Assert.assertEquals(dc.getAliveDestinations().size(), 2);
        dc.removeProxies(addedProxies);
        Assert.assertEquals(dc.getAliveDestinations().size(), 0);
    }
}
