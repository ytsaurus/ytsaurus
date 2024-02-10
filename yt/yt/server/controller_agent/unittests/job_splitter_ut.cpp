#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/controller_agent/config.h>

#include <yt/yt/server/controller_agent/controllers/job_splitter.h>

#include <yt/yt/server/lib/chunk_pools/mock/chunk_pool.h>

#include <yt/yt/server/lib/chunk_pools/public.h>

#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/ytlib/chunk_pools/chunk_stripe.h>

namespace NYT::NControllerAgent::NControllers {
namespace {

using namespace ::testing;
using namespace NLogging;
using namespace NChunkPools;

static const TLogger Logger("JobSplitterTest");

///////////////////////////////////////////////////////////////////////////////

TChunkStripeListPtr CreateTwoRowStripeList()
{
    TChunkStripeListPtr stripeList = New<TChunkStripeList>();
    stripeList->TotalRowCount = 2;
    stripeList->TotalDataWeight = 200;
    return stripeList;
}

TJobSummary CreateOneRowProgressJobSummary(TJobId jobId, bool isSlow = false)
{
    TJobSummary jobSummary;
    jobSummary.Id = jobId;
    jobSummary.TimeStatistics.ExecDuration = TDuration::Seconds(isSlow ? 100 : 1);
    jobSummary.Statistics = std::make_shared<TStatistics>();
    auto& inputDataStatistics = jobSummary.TotalInputDataStatistics.emplace();
    inputDataStatistics.set_row_count(1);
    return jobSummary;
}

TJobSummary CreateNoProgressJobSummary(TJobId jobId)
{
    TJobSummary jobSummary;
    jobSummary.Id = jobId;
    jobSummary.TimeStatistics.PrepareDuration = TDuration::Seconds(100);
    jobSummary.TimeStatistics.ExecDuration = TDuration::Seconds(0);
    jobSummary.Statistics = std::make_shared<TStatistics>();
    auto& inputDataStatistics = jobSummary.TotalInputDataStatistics.emplace();
    inputDataStatistics.set_row_count(0);
    return jobSummary;
}

TCompletedJobSummary CreateCompletedJobSummary(TJobId jobId, TDuration prepareDuration = TDuration::Zero())
{
    TCompletedJobSummary completedJobSummary;
    completedJobSummary.Id = jobId;
    completedJobSummary.TimeStatistics.PrepareDuration = prepareDuration;
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

void OnJobStarted(
    const std::unique_ptr<IJobSplitter>& jobSplitter,
    TJobId jobId,
    const TChunkStripeListPtr& stripeList,
    bool isInterruptible)
{
    jobSplitter->OnJobStarted(jobId, stripeList, jobId.Underlying().Parts64[1], isInterruptible);
}

void LaunchTwoFastJobs(TJobId first, TJobId second, const std::unique_ptr<IJobSplitter>& jobSplitter)
{
    OnJobStarted(jobSplitter, first, CreateTwoRowStripeList(), true);
    jobSplitter->OnJobRunning(CreateOneRowProgressJobSummary(first));

    OnJobStarted(jobSplitter, second, CreateTwoRowStripeList(), true);
    jobSplitter->OnJobRunning(CreateOneRowProgressJobSummary(second));
}

TJobId MakeJobId(ui64 p0, ui64 p1)
{
    return TJobId(TGuid(p0, p1));
}

TJobId MakeResidualSplittableJob(const std::unique_ptr<IJobSplitter>& jobSplitter)
{
    TJobId residualJobId = MakeJobId(0, 0);
    OnJobStarted(jobSplitter, residualJobId, CreateTwoRowStripeList(), true);
    jobSplitter->OnJobRunning(CreateOneRowProgressJobSummary(residualJobId));

    TJobId completedJobId = MakeJobId(0, 1);
    OnJobStarted(jobSplitter, completedJobId, CreateTwoRowStripeList(), true);
    jobSplitter->OnJobCompleted(CreateCompletedJobSummary(completedJobId));

    return residualJobId;
}

TEST(TJobSplitterTest, SplitLongAmongRunningInterruptibleJob)
{
    auto jobSplittingHost = New<TTestJobSplittingBase>();
    auto jobSplitter = CreateJobSplitter(CreateSplitterConfig(), jobSplittingHost.Get(), Logger);

    TJobId slowJobId = MakeJobId(0, 0);
    OnJobStarted(jobSplitter, slowJobId, CreateTwoRowStripeList(), true);
    jobSplitter->OnJobRunning(CreateOneRowProgressJobSummary(slowJobId, /*isSlow*/ true));

    // We need two fast jobs because we compare with median among running jobs.
    LaunchTwoFastJobs(MakeJobId(0, 1), MakeJobId(0, 2), jobSplitter);

    EXPECT_EQ(EJobSplitterVerdict::Split, jobSplitter->ExamineJob(slowJobId));
}

TEST(TJobSplitterTest, SpeculateLongAmongRunningNonInterruptibleJob)
{
    auto jobSplittingHost = New<TChunkPoolJobSplittingHostMock>();
    auto jobSplitter = CreateJobSplitter(CreateSplitterConfig(), jobSplittingHost.Get(), Logger);

    TJobId slowJobId = MakeJobId(0, 0);

    EXPECT_CALL(*jobSplittingHost, IsSplittable(0))
        .WillRepeatedly(Return(false));
    EXPECT_CALL(*jobSplittingHost, IsSplittable(Ne(0)))
        .WillRepeatedly(Return(true));

    OnJobStarted(jobSplitter, slowJobId, CreateTwoRowStripeList(), true);
    jobSplitter->OnJobRunning(CreateOneRowProgressJobSummary(slowJobId, /*isSlow*/ true));

    LaunchTwoFastJobs(MakeJobId(0, 1), MakeJobId(0, 2), jobSplitter);

    EXPECT_EQ(EJobSplitterVerdict::LaunchSpeculative, jobSplitter->ExamineJob(slowJobId));
}

TEST(TJobSplitterTest, SpeculateLongAmongRunningInterruptibleJobWithTooSmallTotalDataWeight)
{
    auto jobSplittingHost = New<TTestJobSplittingBase>();
    auto jobSplitter = CreateJobSplitter(CreateSplitterConfig(), jobSplittingHost.Get(), Logger);

    TJobId slowJobId = MakeJobId(0, 0);
    auto smallStripeList = CreateTwoRowStripeList();
    smallStripeList->TotalDataWeight = 10;
    OnJobStarted(jobSplitter, slowJobId, smallStripeList, true);
    jobSplitter->OnJobRunning(CreateOneRowProgressJobSummary(slowJobId, /*isSlow*/ true));

    LaunchTwoFastJobs(MakeJobId(0, 1), MakeJobId(0, 2), jobSplitter);

    EXPECT_EQ(EJobSplitterVerdict::LaunchSpeculative, jobSplitter->ExamineJob(slowJobId));
}

TEST(TJobSplitterTest, SplitResidualInterruptibleJob)
{
    auto jobSplittingHost = New<TTestJobSplittingBase>();
    auto jobSplitter = CreateJobSplitter(CreateSplitterConfig(), jobSplittingHost.Get(), Logger);

    TJobId residualJobId = MakeJobId(0, 0);
    OnJobStarted(jobSplitter, residualJobId, CreateTwoRowStripeList(), true);
    jobSplitter->OnJobRunning(CreateOneRowProgressJobSummary(residualJobId));

    TJobId completedJobId = MakeJobId(0, 1);
    OnJobStarted(jobSplitter, completedJobId, CreateTwoRowStripeList(), true);
    jobSplitter->OnJobCompleted(CreateCompletedJobSummary(completedJobId));

    EXPECT_EQ(EJobSplitterVerdict::Split, jobSplitter->ExamineJob(residualJobId));
}

TEST(TJobSplitterTest, SpeculateResidualNonInterruptibleJob)
{
    auto jobSplittingHost = New<TChunkPoolJobSplittingHostMock>();
    auto jobSplitter = CreateJobSplitter(CreateSplitterConfig(), jobSplittingHost.Get(), Logger);

    EXPECT_CALL(*jobSplittingHost, IsSplittable(0))
        .WillRepeatedly(Return(false));
    EXPECT_CALL(*jobSplittingHost, IsSplittable(Ne(0)))
        .WillRepeatedly(Return(true));

    TJobId residualJobId = MakeJobId(0, 0);
    OnJobStarted(jobSplitter, residualJobId, CreateTwoRowStripeList(), true);
    jobSplitter->OnJobRunning(CreateOneRowProgressJobSummary(residualJobId));

    TJobId completedJobId = MakeJobId(0, 1);
    OnJobStarted(jobSplitter, completedJobId, CreateTwoRowStripeList(), true);
    jobSplitter->OnJobCompleted(CreateCompletedJobSummary(completedJobId));

    EXPECT_EQ(EJobSplitterVerdict::LaunchSpeculative, jobSplitter->ExamineJob(residualJobId));
}

TEST(TJobSplitterTest, DoNothingWithNotLongAndNotResidualJob)
{
    auto jobSplittingHost = New<TTestJobSplittingBase>();
    auto jobSplitter = CreateJobSplitter(CreateSplitterConfig(), jobSplittingHost.Get(), Logger);

    TJobId slowJobId = MakeJobId(0, 0);
    OnJobStarted(jobSplitter, slowJobId, CreateTwoRowStripeList(), true);
    jobSplitter->OnJobRunning(CreateOneRowProgressJobSummary(slowJobId, /*isSlow*/ false));

    LaunchTwoFastJobs(MakeJobId(0, 1), MakeJobId(0, 2), jobSplitter);

    EXPECT_EQ(EJobSplitterVerdict::DoNothing, jobSplitter->ExamineJob(slowJobId));
}

TEST(TJobSplitterTest, SpeculateLongJobWithNoProgressWhenHasCompletedJobs)
{
    auto jobSplittingHost = New<TTestJobSplittingBase>();
    auto jobSplitter = CreateJobSplitter(CreateSplitterConfig(), jobSplittingHost.Get(), Logger);

    TJobId noProgressJobId = MakeJobId(0, 0);
    OnJobStarted(jobSplitter, noProgressJobId, CreateTwoRowStripeList(), true);
    jobSplitter->OnJobRunning(CreateNoProgressJobSummary(noProgressJobId));

    TJobId completedJobId = MakeJobId(0, 1);
    OnJobStarted(jobSplitter, completedJobId, CreateTwoRowStripeList(), true);
    jobSplitter->OnJobCompleted(CreateCompletedJobSummary(completedJobId, /*prepareDuration*/ TDuration::Seconds(1)));

    EXPECT_EQ(EJobSplitterVerdict::LaunchSpeculative, jobSplitter->ExamineJob(noProgressJobId));
}

TEST(TJobSplitterTest, SpeculateWhenInterruptTimeoutExpired)
{
    auto jobSplittingHost = New<TTestJobSplittingBase>();

    auto config = CreateSplitterConfig();
    config->SplitTimeoutBeforeSpeculate = TDuration::Zero();
    auto jobSplitter = CreateJobSplitter(config, jobSplittingHost.Get(), Logger);

    TJobId residualJobId = MakeResidualSplittableJob(jobSplitter);

    EXPECT_EQ(EJobSplitterVerdict::Split, jobSplitter->ExamineJob(residualJobId));
    EXPECT_EQ(EJobSplitterVerdict::LaunchSpeculative, jobSplitter->ExamineJob(residualJobId));
}

TEST(TJobSplitterTest, JobSplitIsDisabled)
{
    auto jobSplittingHost = New<TTestJobSplittingBase>();

    auto config = CreateSplitterConfig();
    config->EnableJobSplitting = false;
    auto jobSplitter = CreateJobSplitter(config, jobSplittingHost.Get(), Logger);

    TJobId residualJobId = MakeResidualSplittableJob(jobSplitter);
    EXPECT_EQ(EJobSplitterVerdict::LaunchSpeculative, jobSplitter->ExamineJob(residualJobId));
}

TEST(TJobSplitterTest, JobSpeculationIsDisabled)
{
    auto jobSplittingHost = New<TTestJobSplittingBase>();

    auto config = CreateSplitterConfig();
    config->SplitTimeoutBeforeSpeculate = TDuration::Zero();
    config->EnableJobSpeculation = false;
    auto jobSplitter = CreateJobSplitter(config, jobSplittingHost.Get(), Logger);

    TJobId residualJobId = MakeResidualSplittableJob(jobSplitter);
    EXPECT_EQ(EJobSplitterVerdict::Split, jobSplitter->ExamineJob(residualJobId));
    EXPECT_EQ(EJobSplitterVerdict::Split, jobSplitter->ExamineJob(residualJobId));
}

TEST(TJobSplitterTest, EverythingIsDisabled)
{
    auto jobSplittingHost = New<TTestJobSplittingBase>();

    auto config = CreateSplitterConfig();
    config->SplitTimeoutBeforeSpeculate = TDuration::Zero();
    config->EnableJobSplitting = false;
    config->EnableJobSpeculation = false;
    auto jobSplitter = CreateJobSplitter(config, jobSplittingHost.Get(), Logger);

    TJobId residualJobId = MakeResidualSplittableJob(jobSplitter);
    EXPECT_EQ(EJobSplitterVerdict::DoNothing, jobSplitter->ExamineJob(residualJobId));
}

///////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NControllers
