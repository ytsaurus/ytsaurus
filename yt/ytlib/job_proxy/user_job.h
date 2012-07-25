#pragma once

#include "public.h"
#include "job.h"

#include <ytlib/scheduler/job.pb.h>
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
        const NScheduler::NProto::TJobSpec& jobSpec,
        const NScheduler::NProto::TUserJobSpec& userJobSpec,
        TAutoPtr<TUserJobIO> userJobIO);

    NScheduler::NProto::TJobResult Run();

private:
    void InitPipes();
    void ReportStatistic();
    void DoJobIO();

    // Called from forked process.
    void StartJob();
    
    TJobProxyConfigPtr Config;

    TAutoPtr<TUserJobIO> JobIO;
    NScheduler::NProto::TJobSpec JobSpec;
    NScheduler::NProto::TUserJobSpec UserJobSpec;

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
