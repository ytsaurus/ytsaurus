package ru.yandex.devtools.test;

import java.util.List;

import org.junit.platform.engine.FilterResult;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.launcher.PostDiscoveryFilter;

import ru.yandex.devtools.fnmatch.FileNameMatcher;


public class YaFilter implements PostDiscoveryFilter {

    private final YaTestNameBase testName;
    private final List<String> filters;

    public YaFilter(YaTestNameBase testName, List<String> filters) {
        this.testName = testName;
        this.filters = filters;
    }

    public FilterResult apply(TestDescriptor test) {
        if (filters.isEmpty()) {
            return FilterResult.included("");
        }
        if (testName.isTest(test)) {
            for (String pattern : filters) {
                String fullName = testName.getFullName(test);
                FileNameMatcher matcher = new FileNameMatcher(pattern, null);
                matcher.append(fullName);
                if (matcher.isMatch() || pattern.startsWith(fullName)) {
                    return FilterResult.included("passed by " + pattern + " filter");
                }

                TestDescriptor parent = test;
                while (parent.getParent().isPresent()) {
                    parent = parent.getParent().get();
                    if (testName.isTest(parent)) {
                        matcher.reset();
                        fullName = testName.getFullName(parent);
                        matcher.append(fullName);

                        if (matcher.isMatch() || pattern.startsWith(fullName)) {
                            return FilterResult.included("passed by " + pattern + " filter and parent");
                        }
                    }
                }
            }
        }
        return FilterResult.excluded("excluded by all filters");
    }
}
