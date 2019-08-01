#include <yt/core/test_framework/framework.h>

#include <yt/server/controller_agent/config.h>
#include <yt/server/controller_agent/job_splitter.h>
#include <yt/server/lib/chunk_pools/public.h>
#include <yt/server/lib/chunk_pools/chunk_stripe.h>
#include <yt/server/lib/controller_agent/structs.h>

namespace NYT::NControllerAgent {
namespace {

using namespace ::testing;

///////////////////////////////////////////////////////////////////////////////

NChunkPools::TChunkStripeListPtr CreateTwoRowStripeList(bool isSplittable = true)
{
    NChunkPools::TChunkStripeListPtr stripeList = New<NChunkPools::TChunkStripeList>();
    stripeList->TotalRowCount = 2;
    stripeList->TotalDataWeight = 200;
    stripeList->IsSplittable = isSplittable;
    return stripeList;
}

TJobSummary CreateOneRowProgressJobSummary(TJobId jobId, bool isSlow = false)
{
    TJobSummary jobSummary;
    jobSummary.Id = jobId;
    jobSummary.ExecDuration = TDuration::Seconds(isSlow ? 100 : 1);
    jobSummary.Statistics.emplace();
    jobSummary.Statistics->AddSample("/data/input/row_count", 1);
    return jobSummary;
}

TJobSummary CreateNoProgressJobSummary(TJobId jobId)
{
    TJobSummary jobSummary;
    jobSummary.Id = jobId;
    jobSummary.PrepareDuration = TDuration::Seconds(100);
    jobSummary.ExecDuration = TDuration::Seconds(0);
    jobSummary.Statistics.emplace();
    jobSummary.Statistics->AddSample("/data/input/row_count", 0);
    return jobSummary;
}

TCompletedJobSummary CreateCompletedJobSummary(TJobId jobId, TDuration prepareDuration = TDuration::Zero())
{
    TCompletedJobSummary completedJobSummary;
    completedJobSummary.Id = jobId;
    completedJobSummary.PrepareDuration = prepareDuration;
    return completedJobSummary;
}

TJobSplitterConfigPtr CreateSplitterConfig()
{
    TJobSplitterConfigPtr config = New<TJobSplitterConfig>();
    config->MinJobTime = TDuration::Zero();
    config->MinTotalDataWeight = 100;
    config->ResidualJobCountMinThreshold = 0;
    config->UpdatePeriod = TDuration::Zero();
    return config;
}

void LaunchTwoFastJobs(TJobId first, TJobId second, const std::unique_ptr<IJobSplitter>& jobSplitter)
{
    jobSplitter->OnJobStarted(first, CreateTwoRowStripeList(), true);
    jobSplitter->OnJobRunning(CreateOneRowProgressJobSummary(first));

    jobSplitter->OnJobStarted(second, CreateTwoRowStripeList(), true);
    jobSplitter->OnJobRunning(CreateOneRowProgressJobSummary(second));
}

TJobId MakeResidualSplittableJob(const std::unique_ptr<IJobSplitter>& jobSplitter) {
    TJobId residualJobId = TJobId(0, 0);
    jobSplitter->OnJobStarted(residualJobId, CreateTwoRowStripeList(), true);
    jobSplitter->OnJobRunning(CreateOneRowProgressJobSummary(residualJobId));

    TJobId completedJobId = TJobId(0, 1);
    jobSplitter->OnJobStarted(completedJobId, CreateTwoRowStripeList(), true);
    jobSplitter->OnJobCompleted(CreateCompletedJobSummary(completedJobId));

    return residualJobId;
}

TEST(TJobSplitterTest, SplitLongAmongRunningInterruptableJob)
{
    auto jobSplitter = CreateJobSplitter(CreateSplitterConfig(), TOperationId());

    TJobId slowJobId(0, 0);
    jobSplitter->OnJobStarted(slowJobId, CreateTwoRowStripeList(/* isSplittable */ true), true);
    jobSplitter->OnJobRunning(CreateOneRowProgressJobSummary(slowJobId, /* isSlow */ true));

    // We need two fast jobs because we compare with median among running jobs.
    LaunchTwoFastJobs(TJobId(0, 1), TJobId(0, 2), jobSplitter);

    EXPECT_EQ(EJobSplitterVerdict::Split, jobSplitter->ExamineJob(slowJobId));
}

TEST(TJobSplitterTest, SpeculateLongAmongRunningNonInterruptableJob)
{
    auto jobSplitter = CreateJobSplitter(CreateSplitterConfig(), TOperationId());

    TJobId slowJobId(0, 0);
    jobSplitter->OnJobStarted(slowJobId, CreateTwoRowStripeList(/* isSplittable */ false), true);
    jobSplitter->OnJobRunning(CreateOneRowProgressJobSummary(slowJobId, /* isSlow */ true));

    LaunchTwoFastJobs(TJobId(0, 1), TJobId(0, 2), jobSplitter);

    EXPECT_EQ(EJobSplitterVerdict::LaunchSpeculative, jobSplitter->ExamineJob(slowJobId));
}

TEST(TJobSplitterTest, SpeculateLongAmongRunningInterruptableJobWithTooSmallTotalDataWeight)
{
    auto jobSplitter = CreateJobSplitter(CreateSplitterConfig(), TOperationId());

    TJobId slowJobId(0, 0);
    auto smallStripeList = CreateTwoRowStripeList();
    smallStripeList->TotalDataWeight = 10;
    jobSplitter->OnJobStarted(slowJobId, smallStripeList, true);
    jobSplitter->OnJobRunning(CreateOneRowProgressJobSummary(slowJobId, /* isSlow */ true));

    LaunchTwoFastJobs(TJobId(0, 1), TJobId(0, 2), jobSplitter);

    EXPECT_EQ(EJobSplitterVerdict::LaunchSpeculative, jobSplitter->ExamineJob(slowJobId));
}

TEST(TJobSplitterTest, SplitResidualInterruptableJob)
{
    auto jobSplitter = CreateJobSplitter(CreateSplitterConfig(), TOperationId());

    TJobId residualJobId(0, 0);
    jobSplitter->OnJobStarted(residualJobId, CreateTwoRowStripeList(), true);
    jobSplitter->OnJobRunning(CreateOneRowProgressJobSummary(residualJobId));

    TJobId completedJobId = TJobId(0, 1);
    jobSplitter->OnJobStarted(completedJobId, CreateTwoRowStripeList(), true);
    jobSplitter->OnJobCompleted(CreateCompletedJobSummary(completedJobId));

    EXPECT_EQ(EJobSplitterVerdict::Split, jobSplitter->ExamineJob(residualJobId));
}

TEST(TJobSplitterTest, SpeculateResidualNonInterruptableJob)
{
    auto jobSplitter = CreateJobSplitter(CreateSplitterConfig(), TOperationId());

    TJobId residualJobId(0, 0);
    jobSplitter->OnJobStarted(residualJobId, CreateTwoRowStripeList(/* isSplittable */ false), true);
    jobSplitter->OnJobRunning(CreateOneRowProgressJobSummary(residualJobId));

    TJobId completedJobId = TJobId(0, 1);
    jobSplitter->OnJobStarted(completedJobId, CreateTwoRowStripeList(), true);
    jobSplitter->OnJobCompleted(CreateCompletedJobSummary(completedJobId));

    EXPECT_EQ(EJobSplitterVerdict::LaunchSpeculative, jobSplitter->ExamineJob(residualJobId));
}

TEST(TJobSplitterTest, DoNothingWithNotLongAndNotResidualJob)
{
    auto jobSplitter = CreateJobSplitter(CreateSplitterConfig(), TOperationId());

    TJobId slowJobId(0, 0);
    jobSplitter->OnJobStarted(slowJobId, CreateTwoRowStripeList(), true);
    jobSplitter->OnJobRunning(CreateOneRowProgressJobSummary(slowJobId, /* isSlow */ false));

    LaunchTwoFastJobs(TJobId(0, 1), TJobId(0, 2), jobSplitter);

    EXPECT_EQ(EJobSplitterVerdict::DoNothing, jobSplitter->ExamineJob(slowJobId));
}

TEST(TJobSplitterTest, SpeculateLongJobWithNoProgressWhenHasCompletedJobs)
{
    auto jobSplitter = CreateJobSplitter(CreateSplitterConfig(), TOperationId());

    TJobId noProgressJobId(0, 0);
    jobSplitter->OnJobStarted(noProgressJobId, CreateTwoRowStripeList(), true);
    jobSplitter->OnJobRunning(CreateNoProgressJobSummary(noProgressJobId));

    TJobId completedJobId = TJobId(0, 1);
    jobSplitter->OnJobStarted(completedJobId, CreateTwoRowStripeList(), true);
    jobSplitter->OnJobCompleted(CreateCompletedJobSummary(completedJobId, /* prepareDuration */ TDuration::Seconds(1)));

    EXPECT_EQ(EJobSplitterVerdict::LaunchSpeculative, jobSplitter->ExamineJob(noProgressJobId));
}

TEST(TJobSplitterTest, SpeculateWhenInterruptTimeoutExpired)
{
    auto config = CreateSplitterConfig();
    config->SplitTimeoutBeforeSpeculate = TDuration::Zero();
    auto jobSplitter = CreateJobSplitter(config, TOperationId());

    TJobId residualJobId = MakeResidualSplittableJob(jobSplitter);
    
    EXPECT_EQ(EJobSplitterVerdict::Split, jobSplitter->ExamineJob(residualJobId));
    EXPECT_EQ(EJobSplitterVerdict::LaunchSpeculative, jobSplitter->ExamineJob(residualJobId));
}

///////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NY::NChunkPools
