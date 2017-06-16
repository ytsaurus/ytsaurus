#include <yt/core/test_framework/framework.h>

#include <yt/server/scheduler/config.h>
#include <yt/server/scheduler/job_size_adjuster.h>

namespace NYT {
namespace NScheduler {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TJobSizeAdjusterTest, Simple)
{
    i64 dataSizePerJob = 128LL * 1024 * 1024;
    auto config = New<TJobSizeAdjusterConfig>();
    config->MinJobTime = TDuration::Seconds(20);
    config->ExecToPrepareTimeRatio = 10.0;
    auto jobSizeAdjuster = CreateJobSizeAdjuster(
        dataSizePerJob,
        config);

    EXPECT_EQ(dataSizePerJob, jobSizeAdjuster->GetDataSizePerJob());
    i64 jobDataSize = 150LL * 1024 * 1024;

    jobSizeAdjuster->UpdateStatistics(jobDataSize, TDuration::MilliSeconds(20), TDuration::Seconds(19));
    EXPECT_LT(static_cast<i64>(jobDataSize), jobSizeAdjuster->GetDataSizePerJob());
    EXPECT_GT(static_cast<i64>(1.1 * jobDataSize), jobSizeAdjuster->GetDataSizePerJob());

    jobSizeAdjuster->UpdateStatistics(jobDataSize, TDuration::MilliSeconds(20), TDuration::Seconds(1));
    EXPECT_EQ(static_cast<i64>(JobSizeBoostFactor * jobDataSize), jobSizeAdjuster->GetDataSizePerJob());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NScheduler
} // namespace NYT

