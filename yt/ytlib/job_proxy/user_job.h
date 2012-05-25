#pragma once

#include "public.h"
#include "job.h"

#include <ytlib/scheduler/job.pb.h>
#include <ytlib/election/leader_lookup.h>
#include <ytlib/misc/error.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TUserJob
    : public IJob
{
public:
    TUserJob(
        TJobProxyConfigPtr proxyConfig,
        const NScheduler::NProto::TJobSpec& jobSpec);

    NScheduler::NProto::TJobResult Run();

private:
    void InitPipes();
    void ReportStatistic();
    void DoJobIO();

    // Called from forked process.
    void StartJob();
    
    TJobProxyConfigPtr Config;

    TAutoPtr<IUserJobIO> JobIO;
    NScheduler::NProto::TUserJobSpecExt UserJobSpecExt;

    std::vector<TDataPipePtr> Pipes;
    int ActivePipeCount;

    TError JobExitStatus;

    TAutoPtr<TErrorOutput> ErrorOutput;
    std::vector< TAutoPtr<TOutputStream> > TableOutput;

    int ProcessId;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
