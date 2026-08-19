package ru.yandex.devtools.test.containers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.platform.launcher.TestIdentifier;

public class ClassContainer extends JunitEntity {

    private final List<ParametrizedTestContainer> parametrizedTests = new ArrayList<>();
    private final List<TestContainer> tests = new ArrayList<>();

    public ClassContainer(TestIdentifier testIdentifier) {
        super(testIdentifier);
    }

    @Override
    public String getDisplayName() {
        return getTestIdentifier().getLegacyReportingName();
    }

    public void addParametrized(ParametrizedTestContainer parametrized) {
        parametrizedTests.add(parametrized);
    }

    public void addTest(TestContainer test) {
        tests.add(test);
    }

    public List<ParametrizedTestContainer> getParametrizedTests() {
        return Collections.unmodifiableList(parametrizedTests);
    }

    public List<TestContainer> getTests() {
        return Collections.unmodifiableList(tests);
    }

    public String getReportingView() {
        return getDisplayName() + "::";
    }
}

