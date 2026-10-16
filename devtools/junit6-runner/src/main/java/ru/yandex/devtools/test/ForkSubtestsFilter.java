package ru.yandex.devtools.test;

import java.util.List;

import org.junit.platform.launcher.TestIdentifier;


public class ForkSubtestsFilter extends ForkFilter {

    public ForkSubtestsFilter(
            YaTestName testName,
            int modulo,
            int moduloIndex,
            List<TestIdentifier> identifiers) {
        super(testName, modulo, moduloIndex, identifiers, testName::getFullName);
    }

}
