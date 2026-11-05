package ru.yandex.devtools.test;

import org.junit.platform.engine.FilterResult;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.launcher.PostDiscoveryFilter;

public class AlwaysAcceptFilter implements PostDiscoveryFilter {
    @Override
    public FilterResult apply(TestDescriptor object) {
        return FilterResult.included("");
    }
}
