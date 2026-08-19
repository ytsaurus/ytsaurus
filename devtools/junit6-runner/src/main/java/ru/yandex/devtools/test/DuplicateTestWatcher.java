package ru.yandex.devtools.test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class DuplicateTestWatcher {
    private final Map<String, AtomicInteger> duplicateTests = new HashMap<>();

    public String getUniqueTestName(String displayName) {
        int index = duplicateTests.computeIfAbsent(displayName, k -> new AtomicInteger(0))
                .getAndIncrement();
        return displayName + (index > 0 ? "_" + index : "");
    }
}
