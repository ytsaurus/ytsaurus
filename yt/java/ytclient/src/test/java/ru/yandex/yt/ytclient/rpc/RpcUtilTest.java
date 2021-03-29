package ru.yandex.yt.ytclient.rpc;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.hamcrest.core.IsSame;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static ru.yandex.yt.testlib.FutureUtils.getError;
import static ru.yandex.yt.testlib.FutureUtils.waitFuture;

public class RpcUtilTest {
    @Test
    public void withTimeoutTimeoutCompletion() throws InterruptedException {
        var executor = new ScheduledThreadPoolExecutor(1);

        var future = new CompletableFuture<>();

        var futureWithTimeout = RpcUtil.withTimeout(
                future,
                "timeout happened",
                20, TimeUnit.MILLISECONDS,
                executor);

        assertThat(future, IsSame.sameInstance(futureWithTimeout));
        assertThat(future.isDone(), is(false));

        Thread.sleep(5);

        assertThat(future.isDone(), is(false));

        waitFuture(future, 30);

        assertThat(getError(future).getMessage(), containsString("timeout happened"));
    }

    @Test
    public void withTimeoutOkCompletion() throws InterruptedException {
        var executor = new ScheduledThreadPoolExecutor(1);
        executor.setRemoveOnCancelPolicy(true);

        var future = new CompletableFuture<>();

        var futureWithTimeout = RpcUtil.withTimeout(
                future,
                "timeout happened",
                100, TimeUnit.MILLISECONDS,
                executor);

        assertThat(executor.getQueue().size(), greaterThan(0));
        Thread.sleep(5);
        boolean completed = future.complete("ok");
        assertThat(completed, is(true));
        assertThat(executor.getQueue().size(), is(0));
        assertThat(futureWithTimeout.isDone(), is(true));

        Thread.sleep(150);
        assertThat(futureWithTimeout.getNow(null), is("ok"));
    }
}
