package ru.yandex.yt.ytclient.misc;

import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ScheduledSerializedExecutorServiceTest {
    @Test
    public void TestSchedule() {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(10);
        ScheduledSerializedExecutorService executorService = new ScheduledSerializedExecutorService((scheduledExecutorService));

        // Create a bunch of tasks
        long startTime = System.currentTimeMillis();
        ArrayList<Long> executionTimes = new ArrayList<>();
        StringBuilder sb = new StringBuilder();

        ArrayList<ScheduledFuture<?>> runnableFutures = new ArrayList<>();
        ArrayList<ScheduledFuture<Integer>> callableFutures = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            ScheduledFuture<?> f = executorService.schedule(
                    () -> {
                        sb.append('(');
                        executionTimes.add(System.currentTimeMillis());
                        try {
                            Thread.sleep(10, 0);
                        } catch (InterruptedException ex) {
                            fail(ex.toString());
                        }
                        sb.append(')');
                    }, 100, TimeUnit.MILLISECONDS);
            runnableFutures.add(f);
        }

        for (int i = 0; i < 10; ++i) {
            int result = i;
            ScheduledFuture<Integer> f = executorService.schedule(
                    () -> {
                        sb.append('(');
                        executionTimes.add(System.currentTimeMillis());
                        try {
                            Thread.sleep(10, 0);
                        } catch (InterruptedException ex) {
                            fail(ex.toString());
                        }
                        sb.append(')');
                        return result;
                    }, 100, TimeUnit.MILLISECONDS);
            callableFutures.add(f);
        }

        // Wait they are executed
        for (ScheduledFuture<?> f : runnableFutures) {
            try {
                f.get();
            } catch (Exception ex) {
                fail(ex.toString());
            }
        }

        Set<Integer> callableResults = new TreeSet<>();
        for (ScheduledFuture<Integer> f : callableFutures) {
            try {
                callableResults.add(f.get());
            } catch (Exception ex) {
                fail(ex.toString());
            }
        }

        // Check that tasks were executed at the proper time
        assertThat(executionTimes.size(), is(20));

        long rangeBegin = startTime + 95;
        long rangeEnd = startTime + 400;

        for (Long t : executionTimes) {
            assertThat(t, allOf(
                    greaterThan(rangeBegin),
                    lessThan(rangeEnd)));
        }

        // Check that tasks were executed with proper synchronization
        assertThat(sb.toString(), is("()()()()()()()()()()()()()()()()()()()()"));

        // Check callable results
        assertThat(callableResults, is(Set.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)));
    }
}
