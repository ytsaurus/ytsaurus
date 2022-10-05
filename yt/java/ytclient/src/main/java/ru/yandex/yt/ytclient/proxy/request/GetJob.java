package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullFields
@NonNullApi
public class GetJob extends ru.yandex.yt.ytclient.request.GetJob.BuilderBase<GetJob> {

    public GetJob(GUID operationId, GUID jobId) {
        setOperationId(operationId).setJobId(jobId);
    }

    GetJob(String alias, GUID jobId) {
        setOperationAlias(alias).setJobId(jobId);
    }

    public static GetJob fromAlias(String alias, GUID jobId) {
        return new GetJob(alias, jobId);
    }

    @Override
    protected GetJob self() {
        return this;
    }
}
