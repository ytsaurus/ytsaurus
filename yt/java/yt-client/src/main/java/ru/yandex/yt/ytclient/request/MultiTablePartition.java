package ru.yandex.yt.ytclient.request;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
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
