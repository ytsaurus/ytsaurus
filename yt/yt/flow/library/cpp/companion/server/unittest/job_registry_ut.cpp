#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/flow/library/cpp/companion/server/job_registry.h>

namespace NYT::NFlow::NCompanionServer {
namespace {

////////////////////////////////////////////////////////////////////////////////

TJobPtr MakeJob(const TJobId& jobId)
{
    NProto::NCompanion::TJobInfo jobInfo;
    jobInfo.set_spec(R"({computation_class_name = "Shim"})");
    jobInfo.set_dynamic_spec("{}");
    return New<TJob>(jobId, TComputationId("c"), jobInfo);
}

TEST(TJobRegistryTest, PutAndAcquire)
{
    auto registry = New<TJobRegistry>(TDuration::Minutes(10), GetSyncInvoker());
    auto jobId = TJobId(TGuid::Create());

    EXPECT_FALSE(registry->AcquireJob(jobId));

    auto job = MakeJob(jobId);
    registry->PutJob(job);
    auto execution = registry->AcquireJob(jobId);
    ASSERT_TRUE(execution);
    EXPECT_EQ(execution->Job, job);
    registry->ReleaseJob(jobId);

    // The serialization point must not depend on the job instance: a retry
    // carrying the job info replaces the job but keeps its invoker.
    auto replacement = MakeJob(jobId);
    registry->PutJob(replacement);
    auto replaced = registry->AcquireJob(jobId);
    ASSERT_TRUE(replaced);
    EXPECT_EQ(replaced->Job, replacement);
    EXPECT_EQ(replaced->Invoker, execution->Invoker);
    registry->ReleaseJob(jobId);
}

TEST(TJobRegistryTest, ExpiresAfterTtl)
{
    auto registry = New<TJobRegistry>(TDuration::MilliSeconds(50), GetSyncInvoker());
    auto jobId = TJobId(TGuid::Create());
    registry->PutJob(MakeJob(jobId));

    EXPECT_TRUE(registry->AcquireJob(jobId));
    registry->ReleaseJob(jobId);
    Sleep(TDuration::MilliSeconds(100));
    EXPECT_FALSE(registry->AcquireJob(jobId));
}

TEST(TJobRegistryTest, ActivityExtendsTtl)
{
    auto registry = New<TJobRegistry>(TDuration::MilliSeconds(200), GetSyncInvoker());
    auto jobId = TJobId(TGuid::Create());
    registry->PutJob(MakeJob(jobId));

    // Completing a batch within the TTL keeps extending it past the original
    // deadline.
    for (int i = 0; i < 4; ++i) {
        Sleep(TDuration::MilliSeconds(100));
        ASSERT_TRUE(registry->AcquireJob(jobId));
        registry->ReleaseJob(jobId);
    }
    Sleep(TDuration::MilliSeconds(100));
    EXPECT_TRUE(registry->AcquireJob(jobId));
    registry->ReleaseJob(jobId);
}

TEST(TJobRegistryTest, ActiveRequestPreventsExpiration)
{
    auto registry = New<TJobRegistry>(TDuration::MilliSeconds(50), GetSyncInvoker());
    auto jobId = TJobId(TGuid::Create());
    auto otherJobId = TJobId(TGuid::Create());
    auto job = MakeJob(jobId);
    registry->PutJob(job);

    auto execution = registry->AcquireJob(jobId);
    ASSERT_TRUE(execution);
    EXPECT_EQ(execution->Job, job);

    Sleep(TDuration::MilliSeconds(100));
    // PutJob sweeps the whole registry. The active entry and, in particular,
    // its serializing invoker must survive even though its TTL has elapsed.
    registry->PutJob(MakeJob(otherJobId));
    auto concurrent = registry->AcquireJob(jobId);
    ASSERT_TRUE(concurrent);
    EXPECT_EQ(concurrent->Job, job);
    EXPECT_EQ(concurrent->Invoker, execution->Invoker);
    registry->ReleaseJob(jobId);

    registry->ReleaseJob(jobId);
    Sleep(TDuration::MilliSeconds(100));
    EXPECT_FALSE(registry->AcquireJob(jobId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanionServer
