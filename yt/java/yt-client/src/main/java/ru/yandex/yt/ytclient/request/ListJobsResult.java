package ru.yandex.yt.ytclient.request;

import java.util.List;
import java.util.stream.Collectors;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TRspListJobs;

@NonNullApi
@NonNullFields
public class ListJobsResult {
    private final List<JobResult> jobs;

    public ListJobsResult(TRspListJobs rsp) {
        this.jobs = rsp.getResult().getJobsList().stream().map(JobResult::new).collect(Collectors.toList());
    }

    public List<JobResult> getJobs() {
        return jobs;
    }
}
