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
    auto registry = New<TJobRegistry>(GetSyncInvoker());
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

TEST(TJobRegistryTest, RemoveJob)
{
    auto registry = New<TJobRegistry>(GetSyncInvoker());
    auto jobId = TJobId(TGuid::Create());
    auto unknownJobId = TJobId(TGuid::Create());

    // Removal is idempotent: unknown ids are ignored.
    registry->RemoveJob(unknownJobId);

    registry->PutJob(MakeJob(jobId));
    ASSERT_TRUE(registry->AcquireJob(jobId));
    registry->ReleaseJob(jobId);

    registry->RemoveJob(jobId);
    EXPECT_FALSE(registry->AcquireJob(jobId));
    registry->RemoveJob(jobId);

    // A removal must not affect any other job.
    auto otherJobId = TJobId(TGuid::Create());
    registry->PutJob(MakeJob(otherJobId));
    EXPECT_TRUE(registry->AcquireJob(otherJobId));
    registry->ReleaseJob(otherJobId);
}

TEST(TJobRegistryTest, RemovedJobCanBeRegisteredAgain)
{
    auto registry = New<TJobRegistry>(GetSyncInvoker());
    auto jobId = TJobId(TGuid::Create());

    registry->PutJob(MakeJob(jobId));
    registry->RemoveJob(jobId);

    // A registration processed after a removal recreates the entry; if its
    // job is gone from the worker, the reconcile pass reclaims it.
    registry->PutJob(MakeJob(jobId));
    EXPECT_TRUE(registry->AcquireJob(jobId));
    registry->ReleaseJob(jobId);
}

TEST(TJobRegistryTest, PutJobDuringDeferredRemovalRevivesTheEntry)
{
    auto registry = New<TJobRegistry>(GetSyncInvoker());
    auto jobId = TJobId(TGuid::Create());
    registry->PutJob(MakeJob(jobId));

    auto execution = registry->AcquireJob(jobId);
    ASSERT_TRUE(execution);
    registry->RemoveJob(jobId);

    // A registration racing a deferred removal wins: the entry survives the
    // lease release and keeps its serializing invoker.
    registry->PutJob(MakeJob(jobId));
    registry->ReleaseJob(jobId);
    auto revived = registry->AcquireJob(jobId);
    ASSERT_TRUE(revived);
    EXPECT_EQ(revived->Invoker, execution->Invoker);
    registry->ReleaseJob(jobId);
}

TEST(TJobRegistryTest, RemoveJobWithActiveLeaseIsDeferred)
{
    auto registry = New<TJobRegistry>(GetSyncInvoker());
    auto jobId = TJobId(TGuid::Create());
    registry->PutJob(MakeJob(jobId));

    auto execution = registry->AcquireJob(jobId);
    ASSERT_TRUE(execution);

    // Removal must not break the active lease: the entry stops being
    // acquirable but survives until the lease is released.
    registry->RemoveJob(jobId);
    EXPECT_FALSE(registry->AcquireJob(jobId));
    registry->ReleaseJob(jobId);
    EXPECT_FALSE(registry->AcquireJob(jobId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanionServer
