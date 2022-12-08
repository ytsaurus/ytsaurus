package tech.ytsaurus.client.request;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import tech.ytsaurus.core.cypress.YPath;

public class MultiTablePartition {
    private final List<YPath> tableRanges;

    public MultiTablePartition(List<YPath> tableRanges) {
        this.tableRanges = new ArrayList<>(tableRanges);
    }

    public List<YPath> getTableRanges() {
        return tableRanges;
    }

    @Override
    public String toString() {
        return tableRanges.stream().map(Object::toString).collect(Collectors.joining());
    }
}
