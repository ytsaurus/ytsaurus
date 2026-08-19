package ru.yandex.devtools.test.containers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.platform.launcher.TestIdentifier;

public class ParametrizedTestContainer extends JunitEntity {

    private final List<TestContainer> tests = new ArrayList<>();

    public ParametrizedTestContainer(TestIdentifier testIdentifier) {
        super(testIdentifier);
    }

    public void addTest(TestContainer test) {
        tests.add(test);
    }

    public List<TestContainer> getTests() {
        return Collections.unmodifiableList(tests);
    }
}
