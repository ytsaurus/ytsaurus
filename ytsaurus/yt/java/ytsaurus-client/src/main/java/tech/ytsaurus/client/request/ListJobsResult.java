package tech.ytsaurus.client.request;

import java.util.List;
import java.util.stream.Collectors;

import tech.ytsaurus.rpcproxy.TRspListJobs;

public class ListJobsResult {
    private final List<JobResult> jobs;

    public ListJobsResult(TRspListJobs rsp) {
        this.jobs = rsp.getResult().getJobsList().stream().map(JobResult::new).collect(Collectors.toList());
    }

    public List<JobResult> getJobs() {
        return jobs;
    }
}
