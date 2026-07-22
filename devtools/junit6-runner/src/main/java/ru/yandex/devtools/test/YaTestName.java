package ru.yandex.devtools.test;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

public class YaTestName extends CachedTestNames<String, TestIdentifier> {

    // Отслеживает возможные дубликаты в именовании тестов (с одинаковыми displayName) и переименовывает их
    private final Map<String, AtomicInteger> duplicateTests = new HashMap<>();
    private final YaTestNameBase baseName;
    private final TestPlan plan;

    public YaTestName(TestPlan plan) {
        this(new YaTestNameBase(), plan);
    }

    public YaTestName(YaTestNameBase baseName, TestPlan plan) {
        super(baseName);
        this.baseName = Objects.requireNonNull(baseName);
        this.plan = Objects.requireNonNull(plan);
    }

    @Override
    protected String getKeyImpl(TestIdentifier key) {
        return key.getUniqueId();
    }

    @Override
    protected String getClassNameImpl(TestIdentifier test) {
        TestIdentifier parent = plan.getParent(test).orElseThrow(() -> new NoSuchElementException("No parent in test"));
        do {
            if (isClass(parent) || !plan.getParent(parent).isPresent()) {
                return parent.getLegacyReportingName();
            } else {
                parent = plan.getParent(parent).get();
            }
        } while (true);
    }

    @Override
    protected String getMethodNameImpl(TestIdentifier test) {
        String className = getClassName(test);
        String methodName = "";
        TestIdentifier parent = test;
        while (parent != null && !parent.getLegacyReportingName().equals(className)) {
            String displayName = parent.getDisplayName();
            methodName = methodName.isEmpty() ? displayName : (displayName + ":" + methodName);
            parent = plan.getParent(parent).orElse(null);
        }

        int index = duplicateTests.computeIfAbsent(className + "::" + methodName,
                t -> new AtomicInteger()).getAndIncrement();
        if (index > 0) {
            return methodName + "_" + index;
        } else {
            return methodName;
        }
    }

    public boolean isClass(TestIdentifier test) {
        return baseName.isClass(test);
    }

    public boolean isTest(TestDescriptor test) {
        return baseName.isTest(test);
    }


    public boolean isTest(TestIdentifier test) {
        return baseName.isTest(test);
    }

    public Class<?> forName(String name) throws ClassNotFoundException {
        return baseName.forName(name);
    }

}
