package ru.yandex.devtools.test;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.platform.engine.DiscoverySelector;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.TestTag;
import org.junit.platform.engine.discovery.ClassNameFilter;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.engine.support.descriptor.MethodSource;
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
import ru.yandex.devtools.test.containers.ClassContainer;
import ru.yandex.devtools.test.containers.ParametrizedTestContainer;
import ru.yandex.devtools.test.containers.TestContainer;
import ru.yandex.devtools.util.StopWatch;

import static java.util.Collections.emptyList;
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
    protected int listTests(RunnerTask task) throws RuntimeException {
        Parameters params = task.getParams();
        Writer writer = task.getWriter();

        StopWatch cfg = task.getTiming().getConfiguration();
        cfg.start();

        Map<Object, Object> subtestInfo = new HashMap<>();
        Set<ClassContainer> classContainers = listTests(params);
        classContainers.forEach(cls -> {
            cls.getTests().forEach(test -> {
                try {
                    writeTestInfo(subtestInfo, writer, cls, test);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            cls.getParametrizedTests().forEach(param ->
                    param.getTests().forEach(test -> {
                        try {
                            writeTestInfo(subtestInfo, writer, cls, test);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
            );
        });
        cfg.stop();
        return 0;
    }

    private static Set<ClassContainer> listTests(Parameters params) {
        YaTestNameBase baseName = new YaTestNameBase();
        Launcher launcher = LauncherFactory.create();
        LauncherDiscoveryRequest request = getRequestWithForkFilter(launcher, baseName, params);
        TestPlan plan = launcher.discover(request);
        YaTestName testName = new YaTestName(baseName, plan);

        var templateLookup = Junit6TemplateTestLookup.lazyLookup(testName, request, null).get();

        Set<ClassContainer> classContainers = new LinkedHashSet<>();
        for (TestIdentifier root : plan.getRoots()) {
            for (TestIdentifier classIdentifier : plan.getChildren(root)) {
                ClassContainer classContainer = new ClassContainer(classIdentifier);
                classContainers.add(classContainer);

                for (TestIdentifier methodIdentifier : plan.getChildren(classIdentifier)) {
                    var templateInvocations = new ArrayList<TestIdentifier>();
                    if(methodIdentifier.getSource().isPresent()) {
                        TestSource source = methodIdentifier.getSource().get();
                        if (methodIdentifier.isContainer() && source instanceof MethodSource) {
                            templateLookup.discoverTemplateInvocation(methodIdentifier, (MethodSource) source,
                                    templateInvocations::add);
                        }
                    }

                    if (templateInvocations.isEmpty()) {
                        classContainer.addTest(new TestContainer(methodIdentifier, classContainer));
                    } else {
                        var parametrized = new ParametrizedTestContainer(methodIdentifier);
                        classContainer.addParametrized(parametrized);
                        templateInvocations.forEach(invocation ->
                                parametrized.addTest(new TestContainer(invocation, parametrized)));
                    }
                }
            }
        }

        return classContainers;
    }

    private static void writeTestInfo(Map<Object, Object> subtestInfo, Writer writer, ClassContainer classContainer,
                                      TestContainer test) throws IOException {
        subtestInfo.put("test", classContainer.getDisplayName());
        subtestInfo.put("subtest", test.extractMethodDisplayName());
        subtestInfo.put("tags", test.getTestIdentifier()
                .getTags()
                .stream()
                .map(TestTag::getName)
                .collect(Collectors.toSet()));
        writer.write(GSON.toJson(subtestInfo));
        writer.write(System.lineSeparator());
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

        List<DiscoverySelector> selectors = new ArrayList<>();

        if (!params.filters.isEmpty()) {
            var paramsForListing = params.clone();
            paramsForListing.filters = emptyList();
            Set<ClassContainer> classContainers = listTests(paramsForListing);

            YaFilter filter = new YaFilter(classContainers);
            selectors.addAll(filter.filtering(baseName, params.filters));
        } else {
            if (params.testsJar.startsWith("class:")) {
                selectors.add(DiscoverySelectors.selectClass(params.testsJar.substring("class:".length())));
            } else {
                selectors.addAll((DiscoverySelectors.selectClasspathRoots(new HashSet<>(
                        Collections.singletonList(new File(params.testsJar).toPath())))));
            }
        }
        builder.selectors(selectors);
        return builder
                .filters(
                        ClassNameFilter.includeClassNamePatterns(".*"),
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

        if (params.modulo <= 1) {
            return getRequest(baseName, params, new AlwaysAcceptFilter());
        }
        var copyParams = params.clone();
        copyParams.filters = emptyList();
        var request = getRequest(baseName, copyParams, new AlwaysAcceptFilter());
        var plan = launcher.discover(request);
        var testName = new YaTestName(baseName, plan);

        var testIdentifiers = plan.getRoots()
                .stream()
                .flatMap(root -> plan.getDescendants(root).stream())
                .filter(testName::isTest)
                .collect(Collectors.toList());

        PostDiscoveryFilter filter;
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
