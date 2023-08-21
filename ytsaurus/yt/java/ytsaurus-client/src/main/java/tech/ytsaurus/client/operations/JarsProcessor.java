package tech.ytsaurus.client.operations;

import java.util.Collections;
import java.util.Set;

import tech.ytsaurus.client.TransactionalClient;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.lang.NonNullApi;

/**
 * Interface for uploading jars and other files.
 */
@NonNullApi
public interface JarsProcessor {
    /**
     * Detects classpath and uploads it to YT.
     *
     * @return Files that will be copied into task CWD and included in classpath.
     */
    Set<YPath> uploadJars(TransactionalClient yt, MapperOrReducer<?, ?> mapperOrReducer, boolean isLocalMode);

    /**
     * For automatic uploading task resources that are not part of the classpath.
     * You will need your own implementation for this.
     *
     * @return Files that will be copied into task CWD.
     * @see MapperOrReducerSpec#additionalFiles
     */
    default Set<YPath> uploadResources(TransactionalClient yt, MapperOrReducer<?, ?> mapperOrReducer) {
        return Collections.emptySet();
    }
}
