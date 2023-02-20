package ru.yandex.yt.ytclient.proxy.request;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

@NonNullApi
@NonNullFields
public class ConcatenateNodes extends tech.ytsaurus.client.request.ConcatenateNodes.BuilderBase<ConcatenateNodes> {
    public ConcatenateNodes(String[] from, String to) {
        this(
                Arrays.stream(from).map(YPath::simple).collect(Collectors.toList()),
                YPath.simple(to)
        );
    }

    public ConcatenateNodes(List<YPath> source, YPath dest) {
        setSourcePaths(source).setDestinationPath(dest);
    }

    public ConcatenateNodes(tech.ytsaurus.client.request.ConcatenateNodes.BuilderBase<?> builder) {
        super(builder);
    }

    @Override
    protected ConcatenateNodes self() {
        return this;
    }
}
