package ru.yandex.devtools.test;

import java.util.NoSuchElementException;

import org.junit.platform.engine.FilterResult;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.launcher.PostDiscoveryFilter;


/**
 * Old ForkSubtests filter. Keeping it during migration to new ForkFilter implementation.
 */
public class ForkSubtests implements PostDiscoveryFilter {

    private final YaTestNameBase testName;
    private final int modulo, moduloIndex;
    private final boolean enabled;

    public ForkSubtests(YaTestNameBase testName, boolean enabled, int modulo, int moduloIndex) {
        this.testName = testName;
        this.modulo = modulo;
        this.moduloIndex = moduloIndex;
        this.enabled = enabled;
    }

    public FilterResult apply(TestDescriptor test) {
        if (this.modulo > 1 && testName.isTest(test)) {
            TestDescriptor parent = test.getParent().orElseThrow(() ->
                    new NoSuchElementException("No parent in test" + test));
            String name = parent.getDisplayName() + "::" + test.getDisplayName();
            String className = parent.getDisplayName();

            if (this.enabled && Math.abs(name.hashCode()) % modulo == moduloIndex) { //FORK_SUBTESTS
                return FilterResult.included("");
            } else if (!this.enabled && Math.abs(className.hashCode()) % modulo == moduloIndex) { // FORK_TESTS
                return FilterResult.included("");
            } else {
                return FilterResult.excluded("");
            }
        }
        return FilterResult.included("");
    }
}
