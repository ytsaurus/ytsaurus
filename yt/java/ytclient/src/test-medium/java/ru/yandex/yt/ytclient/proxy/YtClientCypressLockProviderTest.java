package ru.yandex.yt.ytclient.proxy;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.yt.ytclient.proxy.locks.ReentrantLockException;
import ru.yandex.yt.ytclient.proxy.request.LockMode;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * This test requires YT_TOKEN environment variable to be set because of the usage of YtClusters.
 */
public class YtClientCypressLockProviderTest extends YtClientTestBase {

    private static final YPath ROOT_PATH = YPath.simple(String.format(
            "//tmp/sprav/tests/ytclient_lock/%s-%d",
            Instant.now(),
            (ThreadLocalRandom.current().nextInt() & 0x7FFFFFFF)
    ));
    @Rule
    public final TestName testName = new TestName();
    private CypressLockProvider provider = null;

    @Before
    public void setup() {
        provider = makeProvider(Duration.ofSeconds(10), Duration.ofSeconds(1));
    }

    private CypressLockProvider makeProvider(Duration pingPeriod, Duration failedPingRetryPeriod) {
        YtClient ytClient = createYtFixture().yt;
        return new CypressLockProvider(ytClient, ROOT_PATH, pingPeriod, failedPingRetryPeriod);
    }

    @Test
    public void createSameLockTwice() {
        String lockName = testName.getMethodName();
        Lock lock1 = provider.createLock(lockName);
        Lock lock2 = provider.createLock(lockName);
        assertNotNull(lock1);
        assertNotNull(lock2);
        assertNotSame(lock1, lock2);
    }

    @Test
    public void tryLockWorks() {
        String lockName = testName.getMethodName();
        Lock lock = provider.createLock(lockName);
        assertTrue(lock.tryLock());
    }

    @Test
    public void trySharedAndSharedLockPossible() {
        String lockName = testName.getMethodName();
        CypressLockProvider.LockImpl lock1 = provider.createLock(lockName, LockMode.Shared);
        CypressLockProvider.LockImpl lock2 = provider.createLock(lockName, LockMode.Shared);
        assertTrue(lock1.tryLock());
        assertTrue(lock1.isTaken());
        assertTrue(lock2.tryLock());
        assertTrue(lock2.isTaken());
    }

    @Test
    public void trySharedAndExclusiveLockIsNotPossible() {
        String lockName = testName.getMethodName();
        CypressLockProvider.LockImpl lock1 = provider.createLock(lockName, LockMode.Shared);
        CypressLockProvider.LockImpl lock2 = provider.createLock(lockName, LockMode.Exclusive);
        assertTrue(lock1.tryLock());
        assertTrue(lock1.isTaken());
        assertFalse(lock2.tryLock());
        assertFalse(lock2.isTaken());
    }

    @Test
    public void tryExclusiveAndSharedLockIsNotPossible() {
        String lockName = testName.getMethodName();
        CypressLockProvider.LockImpl lock1 = provider.createLock(lockName, LockMode.Exclusive);
        CypressLockProvider.LockImpl lock2 = provider.createLock(lockName, LockMode.Shared);
        assertTrue(lock1.tryLock());
        assertTrue(lock1.isTaken());
        assertFalse(lock2.tryLock());
        assertFalse(lock2.isTaken());
    }

    @Test
    public void trySharedAndExclusiveLockIsOkForChildNode() {
        String lockName = testName.getMethodName();
        CypressLockProvider.LockImpl lock1 = provider.createLock(lockName, LockMode.Shared);
        CypressLockProvider.LockImpl lock2 = provider.createLock(lockName + "/sub", LockMode.Exclusive);
        assertTrue(lock1.tryLock());
        assertTrue(lock1.isTaken());
        assertTrue(lock2.tryLock());
        assertTrue(lock2.isTaken());
    }

    @Test
    public void tryExclusiveAndSharedLockIsNotOkForChildNode() {
        String lockName = testName.getMethodName();
        CypressLockProvider.LockImpl lock1 = provider.createLock(lockName, LockMode.Exclusive);
        CypressLockProvider.LockImpl lock2 = provider.createLock(lockName + "/sub", LockMode.Shared);
        assertTrue(lock1.tryLock());
        assertTrue(lock1.isTaken());
        assertTrue(lock2.tryLock());
        assertTrue(lock2.isTaken());
    }

    @Test
    public void tryLockIsNotReentrant() {
        String lockName = testName.getMethodName();

        Lock lock1 = provider.createLock(lockName);
        assertTrue(lock1.tryLock());
        assertFalse(lock1.tryLock());
    }

    @Test
    public void tryLockActuallyLocks() {
        String lockName = testName.getMethodName();

        Lock lock1 = provider.createLock(lockName);
        assertTrue(lock1.tryLock());

        Lock lock2 = provider.createLock(lockName);
        assertFalse(lock2.tryLock());
    }

    @Test
    public void unlockWorks() {
        String lockName = testName.getMethodName();

        Lock lock = provider.createLock(lockName);
        assertTrue(lock.tryLock());

        lock.unlock();

        assertTrue(lock.tryLock());
    }

    @Test
    public void waitingTryLockWorks() throws InterruptedException {
        assertTrue(provider.createLock(testName.getMethodName()).tryLock(100, TimeUnit.MILLISECONDS));
    }

    @Test
    public void waitingTryLockIsNotReentrant() throws InterruptedException {
        String lockName = testName.getMethodName();

        Lock lock1 = provider.createLock(lockName);
        assertTrue(lock1.tryLock(100, TimeUnit.MILLISECONDS));

        assertThrows(ReentrantLockException.class,
                () -> lock1.tryLock(100, TimeUnit.MILLISECONDS));
    }

    @Test
    public void waitingTryLockActuallyLocks() throws InterruptedException {
        String lockName = testName.getMethodName();

        Lock lock1 = provider.createLock(lockName);
        assertTrue(lock1.tryLock(100, TimeUnit.MILLISECONDS));

        Lock lock2 = provider.createLock(lockName);
        assertFalse(lock2.tryLock());
    }

    @Test
    public void waitingTryLockWaits() throws Exception {
        CountDownLatch firstLocked = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);

        Runnable thread1 = () -> {
            try {
                Lock lock = provider.createLock(testName.getMethodName());
                assertTrue(lock.tryLock());
                firstLocked.countDown();
                Thread.sleep(1000);
                lock.unlock();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        Runnable thread2 = () -> {
            try {
                assertTrue(firstLocked.await(2, TimeUnit.SECONDS));

                Lock lock = provider.createLock(testName.getMethodName());

                secondStarted.countDown();
                long startTime = System.currentTimeMillis();
                assertTrue(lock.tryLock(2, TimeUnit.SECONDS));
                assertTrue(System.currentTimeMillis() > startTime + 1000);
                assertTrue(System.currentTimeMillis() < startTime + 2000);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        try {
            Future<?> future1 = executorService.submit(thread1);
            Future<?> future2 = executorService.submit(thread2);

            secondStarted.await(2, TimeUnit.SECONDS);

            future1.get(3, TimeUnit.SECONDS);
            future2.get(3, TimeUnit.SECONDS);
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    public void waitingTryLockTimesOut() throws InterruptedException {
        String lockName = testName.getMethodName();

        Lock lock1 = provider.createLock(lockName);
        assertTrue(lock1.tryLock());

        Lock lock2 = provider.createLock(lockName);
        long startTime = System.currentTimeMillis();
        assertFalse(lock2.tryLock(1500, TimeUnit.MILLISECONDS));
        long duration = System.currentTimeMillis() - startTime;
        assertTrue(duration >= 1500);
        assertTrue(duration <= 2000);
    }

    @Test
    public void waitingTryLockCanWaitLongerThanTransactionTimeout() throws Exception {
        CountDownLatch firstLocked = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);

        int longWait = 3000;
        CypressLockProvider lockProvider = makeProvider(Duration.ofMillis(longWait), Duration.ofSeconds(1));

        Runnable thread1 = () -> {
            try {
                Lock lock = lockProvider.createLock(testName.getMethodName());
                assertTrue(lock.tryLock());
                firstLocked.countDown();
                Thread.sleep(longWait * 3);
                lock.unlock();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        Runnable thread2 = () -> {
            try {
                assertTrue(firstLocked.await(2, TimeUnit.SECONDS));

                Lock lock = lockProvider.createLock(testName.getMethodName());

                secondStarted.countDown();
                long startTime = System.currentTimeMillis();
                assertTrue(lock.tryLock(longWait * 4, TimeUnit.MILLISECONDS));
                assertTrue(System.currentTimeMillis() > startTime + longWait * 3);
                assertTrue(System.currentTimeMillis() < startTime + longWait * 4);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        try {
            Future<?> future1 = executorService.submit(thread1);
            Future<?> future2 = executorService.submit(thread2);

            secondStarted.await(2, TimeUnit.SECONDS);

            future1.get(longWait * 4, TimeUnit.MILLISECONDS);
            future2.get(longWait * 4, TimeUnit.MILLISECONDS);
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    public void lockIsUninterruptible() throws InterruptedException {
        AtomicLong waitTime = new AtomicLong(0);
        CountDownLatch done = new CountDownLatch(1);

        Lock lock1 = provider.createLock(testName.getMethodName());
        lock1.lock();

        Thread thread = new Thread(() -> {
            long start = System.currentTimeMillis();
            try {
                provider.createLock(testName.getMethodName()).lock();
            } catch (Exception e) {
                waitTime.set(-1);
                done.countDown();
                return;
            }
            waitTime.set(System.currentTimeMillis() - start);
            done.countDown();
        });
        thread.start();

        Thread.sleep(1000);
        thread.interrupt();
        Thread.sleep(1000);
        lock1.unlock();

        done.await();

        assertTrue(waitTime.get() > 2000);
    }

    @Test
    public void lockInterruptivelyIsInterruptable() throws InterruptedException {
        AtomicLong waitTime = new AtomicLong(0);
        AtomicBoolean interrupted = new AtomicBoolean(false);
        CountDownLatch threadStarted = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);

        String lockName = testName.getMethodName();
        Lock lock1 = provider.createLock(lockName);
        lock1.lock();

        Thread thread = new Thread(() -> {
            threadStarted.countDown();
            long start = System.currentTimeMillis();
            try {
                provider.createLock(lockName).lockInterruptibly();
            } catch (InterruptedException e) {
                waitTime.set(System.currentTimeMillis() - start);
                interrupted.set(true);
            } catch (Exception e) {
                waitTime.set(-1);
            }
            done.countDown();
        });
        thread.start();
        threadStarted.await();
        Thread.sleep(1000);
        thread.interrupt();
        Thread.sleep(1000);
        lock1.unlock();

        done.await();

        assertTrue(waitTime.get() > 1000);
        assertTrue(interrupted.get());
        assertTrue(waitTime.get() < 2000);
    }

    @Test
    public void lockHoldsLongerThanTransactionTimeout() throws InterruptedException {
        int longWait = 1000;
        CypressLockProvider lockProvider = makeProvider(Duration.ofMillis(longWait), Duration.ofSeconds(10));
        Lock lock = lockProvider.createLock(testName.getMethodName());
        lock.lock();
        try {
            Thread.sleep(longWait * 4);
            assertFalse(lockProvider.createLock(testName.getMethodName()).tryLock());
        } finally {
            lock.unlock();
        }
    }

    @Test
    public void canHaveComplexLockName() {
        assertTrue(provider.createLock(testName.getMethodName() + "/a/b").tryLock());
        assertFalse(provider.createLock(testName.getMethodName() + "/a/b").tryLock());
    }
}
