package ru.yandex.yt.ytclient.proxy.request;

import tech.ytsaurus.core.GUID;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

/**
 * Request for resuming suspended operation
 *
 * @see <a href="https://docs.yandex-team.ru/yt/api/commands#suspend_operation">
 *     resume_operation documentation
 *     </a>
 * @see SuspendOperation
 */
@NonNullApi
@NonNullFields
public class ResumeOperation extends tech.ytsaurus.client.request.ResumeOperation.BuilderBase<ResumeOperation> {

    public ResumeOperation(GUID operationId) {
        setOperationId(operationId);
    }

    ResumeOperation(String operationAlias) {
        setOperationAlias(operationAlias);
    }

    public static ResumeOperation fromAlias(String alias) {
        return new ResumeOperation(alias);
    }

    @Override
    protected ResumeOperation self() {
        return this;
    }
}
