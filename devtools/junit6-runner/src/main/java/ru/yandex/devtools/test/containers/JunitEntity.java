package ru.yandex.devtools.test.containers;

import org.junit.platform.engine.UniqueId;
import org.junit.platform.launcher.TestIdentifier;

import ru.yandex.devtools.test.Shared;

public abstract class JunitEntity {
    private final TestIdentifier testIdentifier;

    protected JunitEntity(TestIdentifier testIdentifier) {
        this.testIdentifier = testIdentifier;
    }

    public String getDisplayName() {
        return Shared.ensureUTF8(testIdentifier.getDisplayName());
    }

    public UniqueId getUniqueId() {
        return testIdentifier.getUniqueIdObject();
    }

    public TestIdentifier getTestIdentifier() {
        return testIdentifier;
    }
}
