package ru.yandex.devtools.test;

import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;
import org.opentest4j.TestAbortedException;

import ru.yandex.devtools.log.Logger;
import ru.yandex.devtools.log.LoggingContext;

public class YaToolTraceListener implements TestExecutionListener {

    private static final Logger logger = Logger.getLogger(YaToolTraceListener.class);

    private final Supplier<Junit6TemplateTestLookup> templateLookup;

    // Все тесты идентифицируются по UniqueId (а он не меняется между вызовами тестов)
    // При нестабильном количестве параметризованных тестов
    private final Set<TestIdentifier> expectedTests = new HashSet<>();

    // Отслеживаем пропуск или падение контейнеров целиком - каждый тест из них будет зарепорчен как
    // failed/skipped соответственно
    private final Map<TestIdentifier, TestExecutionResult> failedContainers = new HashMap<>();
    private final Map<TestIdentifier, String> skippedContainers = new HashMap<>();

    private final Map<TestIdentifier, List<TestIdentifier>> container2Test = new HashMap<>();

    private final TraceListener<String, TestIdentifier> listener;

    public YaToolTraceListener(
            Writer writer, YaTestName testName, LoggingContext loggingContext,
            Supplier<Junit6TemplateTestLookup> templateLookup
    ) {
        this.templateLookup = templateLookup;
        this.listener = new TraceListener<>(loggingContext, writer, testName);
    }

    @Override
    public void testPlanExecutionStarted(TestPlan testPlan) {
        synchronized (listener) {
            logger.info("Test plan execution started");
            for (TestIdentifier root : testPlan.getRoots()) {
                collectTests(root);
                for (TestIdentifier test : testPlan.getDescendants(root)) {
                    collectTests(test);
                }
            }
            Set<TestIdentifier> visited = new HashSet<>(testPlan.getRoots());
            Queue<TestIdentifier> toVisit = new LinkedList<>(testPlan.getRoots());
            while (!toVisit.isEmpty()) {
                TestIdentifier root = toVisit.poll();
                if (root.getType() != TestDescriptor.Type.CONTAINER) {
                    continue;
                }
                for (TestIdentifier test : testPlan.getDescendants(root)) {
                    if (test.getType() == TestDescriptor.Type.CONTAINER && visited.add(test)) {
                        if (!toVisit.offer(test)) {
                            throw new RuntimeException("Internal error: can't add element to queue");
                        }
                    } else if (isTest(test)) {
                        container2Test.computeIfAbsent(root, it -> new ArrayList<>()).add(test);
                    }
                }
            }
            listener.reportChunkStarted();
        }
    }

    @Override
    public void testPlanExecutionFinished(TestPlan testPlan) {
        synchronized (listener) {

            logger.info("Test plan execution finished");

            listener.reportChunkFinished();

            checkExecutionFinished(testPlan, failedContainers, this::checkFailedContainers);
            checkExecutionFinished(testPlan, skippedContainers, this::checkSkippedContainers);

            // Проверили все возможные причины падения и пропусков
            // Любой оставшийся тест - flaky
            for (TestIdentifier testIdentifier : expectedTests) {
                logger.info("testFlaky [%s]", testIdentifier.getDisplayName());
                listener.reportPartiallyFinished(testIdentifier, TestStatus.flaky);
            }
        }
    }


    @Override
    public void executionStarted(TestIdentifier testIdentifier) {
        synchronized (listener) {
            if (isTest(testIdentifier)) {
                listener.reportPreStarted(testIdentifier);
                listener.reportStarted(testIdentifier);

                // Такое возможно, если во время конфигурации параметризованных тестов возникла ошибка
                boolean notExists = expectedTests.add(testIdentifier);
                logger.info("testStarted [%s], %s",
                        testIdentifier.getDisplayName(), notExists ? "unexpected" : "expected");
            }
        }
    }

    @Override
    public void executionSkipped(TestIdentifier testIdentifier, String reason) {
        executionSkipped(testIdentifier, reason, false);
    }

    public void executionSkipped(TestIdentifier testIdentifier, String reason, boolean force) {
        synchronized (listener) {
            if (force || isTest(testIdentifier)) {
                if (isTopLevelPackage(testIdentifier)) {
                    return; // Skip "JUnit Jupiter::ClassParameterizedConstantFailedTest"
                }

                boolean exists = expectedTests.remove(testIdentifier);
                logger.info("testSkipped [%s], %s",
                        testIdentifier.getDisplayName(), exists ? "expected" : "unexpected");
                listener.reportPartiallyFinished(testIdentifier, TestStatus.skipped, reason);
            } else if (testIdentifier.getType() == TestDescriptor.Type.CONTAINER) {
                skippedContainers.put(testIdentifier, reason);
            }
        }
    }

    @Override
    public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult testExecutionResult) {
        executionFinished(testIdentifier, testExecutionResult, false);
    }

    public void executionFinished(
            TestIdentifier testIdentifier,
            TestExecutionResult testExecutionResult,
            boolean force
    ) {
        synchronized (listener) {
            if (force || isTest(testIdentifier)) {
                if (isTopLevelPackage(testIdentifier)) {
                    return; // Skip "JUnit Jupiter::ClassParameterizedConstantFailedTest"
                }
                boolean exists = expectedTests.remove(testIdentifier);
                logger.info("testFinished [%s], %s",
                        testIdentifier.getDisplayName(), exists ? "expected" : "unexpected");
                listener.reportPreFinished(testIdentifier);

                try {
                    listener.reportFinished(testIdentifier, data -> {
                        TestStatus status;
                        if (testExecutionResult.getStatus() == TestExecutionResult.Status.SUCCESSFUL) {
                            // Этот тест никто не ожидал, а он приехал
                            // Такое может быть при нестабильном количестве параметризованных тестов

                            // Как мне кажется, flaky должны быть доп. атрибутом теста
                            status = exists ? TestStatus.good : TestStatus.flaky;
                        } else {
                            Throwable error =
                                    testExecutionResult.getThrowable().orElseThrow(IllegalStateException::new);
                            if (testExecutionResult.getStatus() == TestExecutionResult.Status.FAILED) {
                                status = TestStatus.fail;
                            } else {
                                if (error instanceof TestAbortedException) {
                                    status = TestStatus.skipped;
                                } else {
                                    status = TestStatus.crashed;
                                }
                            }
                            data.put("comment", listener.colorizeStackTrace(error));
                            logger.error("Test case failed with:\n%s", listener.extractStackTrace(error));
                        }
                        return status;
                    });
                } finally {
                    listener.reportReconfigureLoggers();
                }
            } else if (testIdentifier.getType() == TestDescriptor.Type.CONTAINER &&
                    testExecutionResult.getStatus() != TestExecutionResult.Status.SUCCESSFUL) {
                failedContainers.put(testIdentifier, testExecutionResult);
            }
        }
    }

    private boolean isTopLevelPackage(TestIdentifier testIdentifier) {
        String className = listener.getClassName(testIdentifier);
        return "JUnit Jupiter".equals(className);
    }

    private boolean isTest(TestIdentifier testIdentifier) {
        return testIdentifier.isTest();
    }

    private <V> void checkExecutionFinished(
            TestPlan testPlan, Map<TestIdentifier, V> map,
            BiConsumer<TestIdentifier, V> action
    ) {
        Set<TestIdentifier> checkedIdentifiers = new HashSet<>();
        for (Map.Entry<TestIdentifier, V> entry : map.entrySet()) {
            V value = entry.getValue();
            if (checkedIdentifiers.add(entry.getKey())) {
                action.accept(entry.getKey(), value);
                for (TestIdentifier container : testPlan.getDescendants(entry.getKey())) {
                    if (container.getType() == TestDescriptor.Type.CONTAINER && checkedIdentifiers.add(container)) {
                        action.accept(container, value);
                    }
                }
            }
        }
    }

    private void checkFailedContainers(TestIdentifier container, TestExecutionResult result) {
        List<TestIdentifier> testIdentifiers = container2Test.get(container);
        if (testIdentifiers == null) {
            executionFinished(container, result, true);
        } else {
            for (TestIdentifier test : testIdentifiers) {
                if (expectedTests.contains(test)) {
                    executionFinished(test, result);
                }
            }
        }
    }

    private void checkSkippedContainers(TestIdentifier container, String reason) {
        List<TestIdentifier> testIdentifiers = container2Test.get(container);
        if (testIdentifiers == null) {
            executionSkipped(container, reason, true);
        } else {
            for (TestIdentifier test : testIdentifiers) {
                if (expectedTests.contains(test)) {
                    executionSkipped(test, reason);
                }
            }
        }
    }

    private void collectTests(TestIdentifier test) {
        if (test.isContainer()) {
            if (test.getSource().isPresent()) {
                TestSource source = test.getSource().get();
                if (source instanceof MethodSource) {
                    templateLookup.get().discoverTemplateInvocation(test, (MethodSource) source, newTest -> {
                        registerExpectedTest(newTest);
                        container2Test.computeIfAbsent(test, t -> new ArrayList<>()).add(newTest);
                    });
                }
            }
        }

        if (isTest(test)) {
            registerExpectedTest(test);
        }
    }

    private void registerExpectedTest(TestIdentifier testIdentifier) {
        expectedTests.add(testIdentifier);
        logger.info("testRegistered [%s]", testIdentifier.getDisplayName());
        listener.reportPartiallyFinished(testIdentifier, TestStatus.not_launched);
    }

    public TraceListener<String, TestIdentifier> getListener() {
        return listener;
    }
}
