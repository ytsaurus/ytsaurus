package ru.yandex.yt.ytclient.proxy;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.junit.Test;

import ru.yandex.yt.ytclient.proxy.internal.HostPort;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.in;
import static ru.yandex.yt.testlib.FutureUtils.waitOkResult;

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
        assertThat(c1.isDone(), is(true));
        var c2 = nonRepeating.peekClient(releaseFuture);
        assertThat(c2.isDone(), is(true));
        var c3 = nonRepeating.peekClient(releaseFuture);
        assertThat(c3.isDone(), is(true));

        assertThat(
                List.of(
                        Objects.requireNonNull(c1.join().getAddressString()),
                        Objects.requireNonNull(c2.join().getAddressString()),
                        Objects.requireNonNull(c3.join().getAddressString())
                ),
                containsInAnyOrder("localhost:1", "localhost:2", "localhost:3")
        );

        var c4 = nonRepeating.peekClient(releaseFuture);
        assertThat(c4.isDone(), is(true));
        assertThat(c4.join().getAddressString(), is(in(List.of("localhost:1", "localhost:2", "localhost:3"))));
    }
}
