package ru.yandex.yt.ytclient.proxy.request;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
public class ConcatenateNodes extends ru.yandex.yt.ytclient.request.ConcatenateNodes.BuilderBase<
        ConcatenateNodes, ru.yandex.yt.ytclient.request.ConcatenateNodes> {
    public ConcatenateNodes(String[] from, String to) {
        this(
                Arrays.stream(from).map(YPath::simple).collect(Collectors.toList()),
                YPath.simple(to)
        );
    }

    public ConcatenateNodes(List<YPath> source, YPath dest) {
        setSourcePaths(source).setDestinationPath(dest);
    }

    public ConcatenateNodes(ru.yandex.yt.ytclient.request.ConcatenateNodes.BuilderBase<?, ?> builder) {
        super(builder);
    }

    @Override
    protected ConcatenateNodes self() {
        return this;
    }

    @Override
    public ru.yandex.yt.ytclient.request.ConcatenateNodes build() {
        return new ru.yandex.yt.ytclient.request.ConcatenateNodes(this);
    }
}
