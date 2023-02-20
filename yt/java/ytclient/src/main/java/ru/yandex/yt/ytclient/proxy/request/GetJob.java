package ru.yandex.yt.ytclient.proxy.request;

import tech.ytsaurus.core.GUID;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

@NonNullFields
@NonNullApi
public class GetJob extends tech.ytsaurus.client.request.GetJob.BuilderBase<GetJob> {

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
