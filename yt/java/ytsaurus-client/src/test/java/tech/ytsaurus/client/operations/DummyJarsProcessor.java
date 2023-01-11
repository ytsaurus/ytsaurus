package tech.ytsaurus.client.operations;

import java.util.Collections;
import java.util.Set;

import tech.ytsaurus.client.TransactionalClient;
import tech.ytsaurus.core.cypress.YPath;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
public class DummyJarsProcessor implements JarsProcessor {
    @Override
    public Set<YPath> uploadJars(
            TransactionalClient yt, MapperOrReducer<?, ?> mapperOrReducer, boolean isLocalMode) {
        return Collections.emptySet();
    }
}
