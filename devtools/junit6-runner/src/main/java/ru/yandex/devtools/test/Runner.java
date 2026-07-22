package ru.yandex.devtools.test;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.platform.engine.TestTag;
import org.junit.platform.engine.discovery.ClassNameFilter;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.PostDiscoveryFilter;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;

import ru.yandex.devtools.log.Logger;
import ru.yandex.devtools.test.Shared.Parameters;
import ru.yandex.devtools.util.StopWatch;

import static ru.yandex.devtools.test.Shared.GSON;

public class Runner extends AbstractRunner {

    private static final Logger logger = Logger.getLogger(Runner.class);

    // For backward compatibility only
    public static Parameters params;

    @Override
    protected void testCompatibility() {
        try {
            Class.forName("org.junit.runner.JUnitCore");
            try {
                Class.forName("org.junit.vintage.engine.VintageTestEngine");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("JUnit 4 classpath detected in JUnit 6 Test Launcher " +
                        "but no JUnit 6 Vintage Engine found");
            }
        } catch (ClassNotFoundException e) {
            // OK
        }
    }

    @Override
    protected void setParams(Parameters params) {
        Runner.params = params;
    }

    @Override
    protected boolean isLegacy() {
        return false;
    }

    @Override
    protected String getName() {
        return "JUnit 6";
    }

    @Override
    protected int listTests(RunnerTask task) throws IOException {
        Parameters params = task.getParams();
        Writer writer = task.getWriter();

        StopWatch cfg = task.getTiming().getConfiguration();
        cfg.start();

        YaTestNameBase baseName = new YaTestNameBase();
        Launcher launcher = LauncherFactory.create();
        LauncherDiscoveryRequest request = getRequestWithForkFilter(launcher, baseName, params);
        TestPlan plan = launcher.discover(request);
        YaTestName testName = new YaTestName(baseName, plan);

        Map<Object, Object> subtestInfo = new HashMap<>();
        for (TestIdentifier root : plan.getRoots()) {
            logger.info("Root: %s", root);

            for (TestIdentifier test : plan.getDescendants(root)) {
                logger.info("Found test: %s", test);

                if (testName.isTest(test)) {
                    subtestInfo.put("test", testName.getClassName(test));
                    subtestInfo.put("subtest", testName.getMethodName(test));
                    subtestInfo.put("tags", test.getTags().stream().map(TestTag::getName).collect(Collectors.toSet()));
                    writer.write(GSON.toJson(subtestInfo));
                    writer.write(System.lineSeparator());
                }
            }
        }
        cfg.stop();
        return 0;
    }


    @Override
    protected int executeTests(RunnerTask task) throws Exception {
        Parameters params = task.getParams();

        StopWatch cfg = task.getTiming().getConfiguration();
        cfg.start();

        if (Shared.loadFilters(params)) {
            params.forkSubtests = false;
        }

        YaTestNameBase baseName = new YaTestNameBase();
        Launcher launcher = LauncherFactory.create();
        LauncherDiscoveryRequest request = getRequestWithForkFilter(launcher, baseName, params);
        TestPlan plan = launcher.discover(request);

        YaTestName testName = new YaTestName(baseName, plan);

        String outputRoot = params.testOutputsRoot != null ? params.testOutputsRoot : "";
        YaToolTraceListener listener = new YaToolTraceListener(task.getWriter(), testName, task.getLoggingContext(),
                Junit6TemplateTestLookup.lazyLookup(testName, request, Path.of(outputRoot)));

        TraceListener<String, TestIdentifier> traceListener = listener.getListener();

        Canonizer.setListener(traceListener.getCanonizingListener());
        Metrics.setListener(traceListener.getMetricsListener());
        Links.setListener(traceListener.getLinksListener());

        cfg.stop();

        StopWatch exec = task.getTiming().getExecution();
        exec.start();
        if (params.allure) {
            tryExecuteWithAllure(launcher, plan, listener);
        } else {
            launcher.execute(plan, listener);
        }
        exec.stop();

        return 0;
    }

    static void tryExecuteWithAllure(Launcher launcher, TestPlan plan,
                                     TestExecutionListener listener) throws Exception {
        Class<?> allureListenerClass = null;
        try {
            allureListenerClass = Class.forName("io.qameta.allure.junitplatform.AllureJunitPlatform");
        } catch (ClassNotFoundException e) {
            logger.info("No allure junit listener found in classpath");
        }
        if (allureListenerClass != null) {
            Object allureListener = allureListenerClass.getDeclaredConstructor().newInstance();
            launcher.execute(plan, listener, (TestExecutionListener) allureListener);
        } else {
            launcher.execute(plan, listener);
        }
    }


    static LauncherDiscoveryRequest getRequest(YaTestNameBase baseName, Parameters params,
                                               PostDiscoveryFilter additionalFilter) {
        LauncherDiscoveryRequestBuilder builder = LauncherDiscoveryRequestBuilder.request();
        if (params.testsJar.startsWith("class:")) {
            builder.selectors(DiscoverySelectors.selectClass(params.testsJar.substring("class:".length())));
        } else {
            builder.selectors(DiscoverySelectors.selectClasspathRoots(new HashSet<>(
                    Collections.singletonList(new File(params.testsJar).toPath()))));
        }

        return builder
                .filters(
                        ClassNameFilter.includeClassNamePatterns(".*"),
                        new YaFilter(baseName, params.filters),
                        new RuntimeTagFilter(baseName, params.junit_tags),
                        additionalFilter
                )
                .build();
    }


    static LauncherDiscoveryRequest getRequestWithForkFilter(Launcher launcher, YaTestNameBase baseName,
                                                             Parameters params) {
        if (!params.experimentalFork) {
            var filter = new ForkSubtests(baseName, params.forkSubtests, params.modulo, params.moduloIndex);
            return getRequest(baseName, params, filter);
        }

        var request = getRequest(baseName, params, new AlwaysAcceptFilter());
        if (params.modulo <= 1) {
            return request;
        }

        var plan = launcher.discover(request);
        var testName = new YaTestName(baseName, plan);

        var testIdentifiers = plan.getRoots()
                .stream()
                .flatMap(root -> plan.getDescendants(root).stream())
                .filter(testName::isTest)
                .collect(Collectors.toList());

        PostDiscoveryFilter filter = null;
        if (params.forkSubtests) {
            filter = new ForkSubtestsFilter(testName, params.modulo, params.moduloIndex, testIdentifiers);
        } else {
            filter = new ForkTestsFilter(testName, params.modulo, params.moduloIndex, testIdentifiers);
        }

        return getRequest(baseName, params, filter);
    }


    public static void main(String[] args) throws Exception {
        new Runner().run(args);
    }

}
