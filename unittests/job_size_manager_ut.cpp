#include "framework.h"

#include <yt/server/scheduler/config.h>
#include <yt/server/scheduler/job_size_manager.h>

namespace NYT {
namespace NScheduler {
namespace {

///////////////////////////////////////////////////////////////////////////////

TEST(TJobSizeManagerTest, Simple)
{
    i64 dataSizePerJob = 128LL * 1024 * 1024;
    i64 maxDataSizePerJob = 16LL * 1024 * 1024 * 1024;
    auto config = New<TJobSizeManagerConfig>();
    config->MinJobTime = TDuration::Seconds(20);
    config->ExecToPrepareTimeRatio = 10.0;
    auto jobSizeManager = CreateJobSizeManager(
        dataSizePerJob,
        maxDataSizePerJob,
        config);

    EXPECT_EQ(dataSizePerJob, jobSizeManager->GetIdealDataSizePerJob());
    i64 jobDataSize = 150LL * 1024 * 1024;

    jobSizeManager->OnJobCompleted(jobDataSize, TDuration::MilliSeconds(20), TDuration::Seconds(19));
    EXPECT_LT(static_cast<i64>(jobDataSize), jobSizeManager->GetIdealDataSizePerJob());
    EXPECT_GT(static_cast<i64>(1.1 * jobDataSize), jobSizeManager->GetIdealDataSizePerJob());

    jobSizeManager->OnJobCompleted(jobDataSize, TDuration::MilliSeconds(20), TDuration::Seconds(1));
    EXPECT_EQ(static_cast<i64>(JobSizeBoostFactor * jobDataSize), jobSizeManager->GetIdealDataSizePerJob());
}

///////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NScheduler
} // namespace NYT

