package tech.ytsaurus.client.rpc;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static tech.ytsaurus.testlib.FutureUtils.getError;
import static tech.ytsaurus.testlib.FutureUtils.waitFuture;

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

        assertSame(future, futureWithTimeout);
        assertFalse(future.isDone());

        Thread.sleep(5);

        assertFalse(future.isDone());

        waitFuture(future, 30);

        assertTrue(getError(future).getMessage().contains("timeout happened"));
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

        assertTrue(executor.getQueue().size() > 0);
        Thread.sleep(5);
        boolean completed = future.complete("ok");
        assertTrue(completed);
        assertEquals(executor.getQueue().size(), 0);
        assertTrue(futureWithTimeout.isDone());

        Thread.sleep(150);
        assertEquals(futureWithTimeout.getNow(null), "ok");
    }
}
