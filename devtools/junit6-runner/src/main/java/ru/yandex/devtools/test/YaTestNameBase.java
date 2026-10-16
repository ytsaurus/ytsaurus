package ru.yandex.devtools.test;

import java.util.NoSuchElementException;

import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.launcher.TestIdentifier;

public class YaTestNameBase extends CachedTestNames<UniqueId, TestDescriptor> {

    @Override
    protected UniqueId getKeyImpl(TestDescriptor key) {
        return key.getUniqueId();
    }

    @Override
    protected String getClassNameImpl(TestDescriptor test) {
        TestDescriptor parent = test.getParent().orElseThrow(() ->
                new NoSuchElementException("No parent in test " + test));
        do {
            if (isClass(parent) || !parent.getParent().isPresent()) {
                return parent.getLegacyReportingName();
            } else {
                parent = parent.getParent().get();
            }
        } while (true);
    }

    @Override
    protected String getMethodNameImpl(TestDescriptor test) {
        String className = getClassName(test);
        String methodName = "";
        TestDescriptor parent = test;
        while (parent != null && !parent.getLegacyReportingName().equals(className)) {
            String displayName = parent.getDisplayName();
            methodName = methodName.isEmpty() ? displayName : (displayName + ":" + methodName);
            parent = parent.getParent().orElse(null);
        }
        return methodName;
    }

    public boolean isClass(TestDescriptor test) {
        String name = test.getLegacyReportingName();
        return isClassName(name);
    }

    public boolean isClass(TestIdentifier test) {
        String name = test.getLegacyReportingName();
        return isClassName(name);
    }

    public boolean isTest(TestDescriptor test) {
        if (!test.getParent().isPresent()) { // This is definitely not a test
            return false;
        }
        return test.getType().isTest() ||
                test.getType().isContainer() && !isClass(test);
    }

    public boolean isTest(TestIdentifier test) {
        return test.getType().isTest() ||
                test.getType().isContainer() && !isClass(test);
    }

}
