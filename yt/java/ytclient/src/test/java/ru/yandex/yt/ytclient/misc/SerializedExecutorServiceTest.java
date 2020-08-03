package ru.yandex.yt.ytclient.misc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class SerializedExecutorServiceTest {
    @Test
    public void TestCommonExecution() {
        ConcurrentLinkedQueue<Character> queue = new ConcurrentLinkedQueue<>();
        ExecutorService threadPool = new ScheduledThreadPoolExecutor(10);
        ExecutorService service = new SerializedExecutorService(threadPool);

        List<Future<Void>> futures = new ArrayList<>();

        for (int i = 0; i != 20; ++i) {
            futures.add(service.submit(() -> {
                queue.offer('(');
                Thread.sleep(10, 0);
                queue.offer(')');
                return null;
            }));
        }

        for (Future<Void> f : futures) {
            try {
                f.get();
            } catch (Exception e) {
                fail("unexpected exception: " + e);
            }
        }
        service.shutdownNow();

        StringBuilder sb = new StringBuilder();
        for (Character c : queue) {
            sb.append(c);
        }

        assertThat(sb.toString(), is("()()()()()()()()()()()()()()()()()()()()"));
    }

    @Test
    public void TestCancelation() {
        ExecutorService threadPool = new ScheduledThreadPoolExecutor(10);
        SerializedExecutorService service = new SerializedExecutorService(threadPool);

        final int INITIAL = 0;
        final int INSIDE_TASK1 = 1;
        final int CANCELED = 2;
        final int TEST_ERROR = 3;

        AtomicInteger step = new AtomicInteger(INITIAL);
        service.submit(() -> {
            step.set(INSIDE_TASK1);

            //noinspection StatementWithEmptyBody
            while (step.get() != CANCELED) {
                // busy wait
            }
            return null;
        });

        //noinspection StatementWithEmptyBody
        while (step.get() != 1) {
            // busy wait
        }

        CompletableFuture<Void> fut2 = service.submit(() -> {
            step.set(TEST_ERROR);
            return null;
        });

        fut2.cancel(true);
        step.set(CANCELED);

        service.shutdown();
        assertThat(step.get(), is(CANCELED));
    }
}
