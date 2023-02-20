package ru.yandex.yt.ytclient.proxy.request;

import tech.ytsaurus.core.GUID;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

/**
 * Request for suspending operation
 *
 * @see <a href="https://docs.yandex-team.ru/yt/api/commands#suspend_operation">
 *     suspend_operation documentation
 *     </a>
 * @see ResumeOperation
 */
@NonNullApi
@NonNullFields
public class SuspendOperation extends tech.ytsaurus.client.request.SuspendOperation.BuilderBase<SuspendOperation> {

    /**
     * Construct request from operation id.
     */
    public SuspendOperation(GUID operationId) {
        setOperationId(operationId);
    }

    SuspendOperation(String operationAlias) {
        setOperationAlias(operationAlias);
    }

    /**
     * Construct request from operation alias
     */
    public static SuspendOperation fromAlias(String operationAlias) {
        return new SuspendOperation(operationAlias);
    }

    @Override
    protected SuspendOperation self() {
        return this;
    }
}
