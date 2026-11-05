package ru.yandex.devtools.test.containers;

import java.util.Objects;

import org.junit.platform.launcher.TestIdentifier;

public class TestContainer extends JunitEntity {

    private final JunitEntity parent;

    public TestContainer(TestIdentifier testIdentifier, JunitEntity parent) {
        super(testIdentifier);

        Objects.requireNonNull(parent, "Test container should have a parent");
        if (parent instanceof TestContainer) {
            throw new IllegalArgumentException("Parent must be not a TestContainer");
        }
        this.parent = parent;
    }

    public String extractMethodDisplayName() {
        if (getParent() instanceof ClassContainer) {
            return getDisplayName();
        } else {
            return getParent().getDisplayName() + ":" + getDisplayName();
        }
    }

    public JunitEntity getParent() {
        return parent;
    }
}

