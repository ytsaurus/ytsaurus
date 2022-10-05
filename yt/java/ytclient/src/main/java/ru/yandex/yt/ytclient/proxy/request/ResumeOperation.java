package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

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
public class ResumeOperation extends ru.yandex.yt.ytclient.request.ResumeOperation.BuilderBase<ResumeOperation> {

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
