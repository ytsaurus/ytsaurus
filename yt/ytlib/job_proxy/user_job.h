#pragma once

#include "public.h"
#include "job.h"

#include <ytlib/scheduler/jobs.pb.h>
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
        const TJobIoConfigPtr& ioConfig,
        const NElection::TLeaderLookup::TConfig::TPtr& masterConfig,
        const NScheduler::NProto::TJobSpec& jobSpec);

    NScheduler::NProto::TJobResult Run();

private:
    void InitPipes();
    void ReportStatistic();
    void DoJobIO();

    // Called from forked process.
    void StartJob();

    TAutoPtr<IUserJobIo> JobIo;
    NScheduler::NProto::TUserJobSpec UserJobSpec;

    std::vector<TDataPipePtr> DataPipes;
    int ActivePipesCount;

    TError JobExitStatus;

    int ProcessId;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
