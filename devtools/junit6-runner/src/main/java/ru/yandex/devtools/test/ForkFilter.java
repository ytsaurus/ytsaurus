package ru.yandex.devtools.test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.platform.engine.FilterResult;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.launcher.PostDiscoveryFilter;
import org.junit.platform.launcher.TestIdentifier;

public class ForkFilter implements PostDiscoveryFilter {

    private final YaTestName testName;
    private final int modulo;
    private final Set<String> includedNames;
    private final Set<String> allNames;
    private final Function<TestIdentifier, String> nameExtractor;

    public ForkFilter(
            YaTestName testName,
            int modulo,
            int moduloIndex,
            List<TestIdentifier> identifiers,
            Function<TestIdentifier, String> nameExtractor) {
        this.testName = testName;
        this.modulo = modulo;
        this.nameExtractor = nameExtractor;
        this.allNames = getAllNames(testName, identifiers, nameExtractor);
        this.includedNames = getIncludedNames(modulo, moduloIndex, new ArrayList<>(this.allNames));
    }

    public FilterResult apply(TestDescriptor test) {
        if (modulo > 1 && testName.isTest(test)) {
            var name = nameExtractor.apply(TestIdentifier.from(test));
            if (includedNames.contains(name)) {
                return FilterResult.included("");
            } else {
                if (!allNames.contains(name)) {
                    // Just to be sure that test is not lost
                    throw new RuntimeException("Broken state during tests distribution to chunks. Unexpected test: " + name);
                }

                return FilterResult.excluded("");
            }
        }
        return FilterResult.included("");
    }

    private static Set<String> getAllNames(
            YaTestName testName,
            List<TestIdentifier> identifiers,
            Function<TestIdentifier, String> nameExtractor
    ) {
        return identifiers.stream()
                .filter(testName::isTest)
                .map(nameExtractor)
                .sorted()
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static Set<String> getIncludedNames(int modulo, int moduloIndex, List<String> allNames) {
        if (modulo > 1) {
            var minIntervalSize = allNames.size() / modulo;
            var remainder = allNames.size() % modulo;

            var startIndex = minIntervalSize * moduloIndex + Math.min(remainder, moduloIndex);
            var endIndex = startIndex + (moduloIndex < remainder ? minIntervalSize + 1 : minIntervalSize);

            return new HashSet<>(allNames.subList(startIndex, Math.min(endIndex, allNames.size())));
        }

        return new HashSet<>(allNames);
    }

}
