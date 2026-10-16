package ru.yandex.devtools.test;

import java.util.List;

import org.junit.platform.launcher.TestIdentifier;

public class ForkTestsFilter extends ForkFilter {

    public ForkTestsFilter(
            YaTestName testName,
            int modulo,
            int moduloIndex,
            List<TestIdentifier> descriptors) {
        super(testName, modulo, moduloIndex, descriptors, testName::getClassName);
    }

}
