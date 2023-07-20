package tech.ytsaurus.client;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.junit.Assert;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static tech.ytsaurus.testlib.FutureUtils.waitOkResult;

public class NonRepeatingClientPoolTest extends ClientPoolTestBase {
    @Test
    public void generalTest() {
        var clientPool = newClientPool();
        waitOkResult(
                clientPool.updateClients(
                        List.of(
                                HostPort.parse("localhost:1"),
                                HostPort.parse("localhost:2"),
                                HostPort.parse("localhost:3")
                        )),
                100
        );

        var nonRepeating = new NonRepeatingClientPool(clientPool);
        var releaseFuture = new CompletableFuture<Void>();

        var c1 = nonRepeating.peekClient(releaseFuture);
        assertTrue(c1.isDone());
        var c2 = nonRepeating.peekClient(releaseFuture);
        assertTrue(c2.isDone());
        var c3 = nonRepeating.peekClient(releaseFuture);
        assertTrue(c3.isDone());

        var data = List.of(
                Objects.requireNonNull(c1.join().getAddressString()),
                Objects.requireNonNull(c2.join().getAddressString()),
                Objects.requireNonNull(c3.join().getAddressString())
        );
        Assert.assertTrue(data.contains("localhost:1"));
        Assert.assertTrue(data.contains("localhost:2"));
        Assert.assertTrue(data.contains("localhost:3"));

        var c4 = nonRepeating.peekClient(releaseFuture);
        assertTrue(c4.isDone());
        assertTrue(List.of("localhost:1", "localhost:2", "localhost:3").contains(c4.join().getAddressString()));
    }
}
