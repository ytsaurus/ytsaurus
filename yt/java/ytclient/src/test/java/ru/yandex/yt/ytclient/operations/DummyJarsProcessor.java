package ru.yandex.yt.ytclient.operations;

import java.util.Collections;
import java.util.Set;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.yt.ytclient.proxy.TransactionalClient;

public class DummyJarsProcessor implements JarsProcessor {
    @Override
    public Set<YPath> uploadJars(
            TransactionalClient yt, MapperOrReducer mapperOrReducer, boolean isLocalMode) {
        return Collections.emptySet();
    }
}
