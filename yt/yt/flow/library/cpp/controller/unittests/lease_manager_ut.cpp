#include <yt/yt/flow/library/cpp/controller/config.h>
#include <yt/yt/flow/library/cpp/controller/lease_manager.h>

#include <yt/yt/flow/library/cpp/controller/unittests/mock/resource_balancer_helpers.h>
#include <yt/yt/flow/library/cpp/controller/unittests/mock/yt_connector.h>

#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/transaction.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow::NController {
namespace {

using namespace NApi;
using namespace NBalancer::NTesting;
using namespace NConcurrency;

using ::testing::_;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::StrictMock;

////////////////////////////////////////////////////////////////////////////////

class TChaosLeaseManagerTest
    : public ::testing::Test
{
protected:
    const TLeaseId LeaseId_ = TLeaseId(TGuid::Create());
    const TError TransientError_ = TError("Chaos cell is unavailable");

    const TIntrusivePtr<StrictMock<TMockClient>> Client_ = New<StrictMock<TMockClient>>();
    const TActionQueuePtr ActionQueue_ = New<TActionQueue>("LeasePing");

    ILeaseManagerPtr LeaseManager_;

    void SetUp() override
    {
        auto connector = New<StrictMock<TMockYTConnector>>();
        EXPECT_CALL(*connector, GetClient())
            .WillRepeatedly(Return(Client_));

        auto config = New<TLeaseManagerConfig>();
        config->TerminationBackoff = {
            .InvocationCount = 1,
            .MinBackoff = TDuration::MilliSeconds(1),
            .MaxBackoff = TDuration::MilliSeconds(1),
        };
        auto chaosConfig = New<TChaosElectionBackendConfig>();
        chaosConfig->ChaosCellBundle = "bundle";
        LeaseManager_ = CreateLeaseManager(
            connector,
            config,
            EElectionBackend::Chaos,
            chaosConfig,
            /*maxWritesPerTransaction*/ 1,
            ActionQueue_->GetInvoker());
    }

    void TearDown() override
    {
        ActionQueue_->Shutdown();
    }

    //! A partition with a job per lease. Each iteration starts from the persisted layout: a
    //! discarded one leaves nothing behind.
    static TFlowViewPtr MakeFlowView(const std::vector<TLeaseId>& leaseIds)
    {
        auto flowView = MakeEmptyFlowView();
        for (int index = 0; index < std::ssize(leaseIds); ++index) {
            auto partitionId = MakePartitionId(index + 1);
            AddPartition(flowView, partitionId, MakeComputationId("computation"));
            auto job = New<TJob>();
            job->JobId = TJobId(TGuid::Create());
            job->PartitionId = partitionId;
            job->LeaseId = leaseIds[index];
            flowView->State->StartMutation();
            flowView->State->ExecutionSpec->Layout->CreateJob(job);
            flowView->State->CommitMutation();
        }
        return flowView;
    }

    static void RemoveJobs(const TFlowViewPtr& flowView)
    {
        const auto& layout = flowView->State->ExecutionSpec->Layout;
        std::vector<TJobId> jobIds;
        for (const auto& [jobId, job] : layout->Jobs) {
            jobIds.push_back(jobId);
        }

        for (const auto& jobId : jobIds) {
            layout->RemoveJob(jobId, EJobFinishReason::Stopped);
        }
    }

    static TIntrusivePtr<StrictMock<TMockTransaction>> MakeLease(TLeaseId leaseId, ITransaction::TAbortedHandler* aborted)
    {
        auto lease = New<StrictMock<TMockTransaction>>();
        EXPECT_CALL(*lease, GetId())
            .WillRepeatedly(Return(leaseId));
        EXPECT_CALL(*lease, SubscribeAborted(_))
            .WillOnce(SaveArg<0>(aborted));
        EXPECT_CALL(*lease, Ping(_))
            .WillRepeatedly(Return(OKFuture));
        return lease;
    }
};

TEST_F(TChaosLeaseManagerTest, StrayLeaseIsTerminatedWithinOneIteration)
{
    auto aborted = std::make_shared<ITransaction::TAbortedHandler>();
    auto lease = MakeLease(LeaseId_, aborted.get());
    EXPECT_CALL(*lease, Abort(_))
        .WillOnce(Return(MakeFuture(TransientError_)))
        .WillOnce([aborted] (const TTransactionAbortOptions& /*options*/) {
            (*aborted)(TError("Lease aborted"));
            return OKFuture;
        });

    EXPECT_CALL(*Client_, AttachChaosLease(LeaseId_, _))
        .WillOnce(Return(MakeFuture<IPrerequisitePtr>(TransientError_)))
        .WillOnce(Return(MakeFuture<IPrerequisitePtr>(lease)));

    auto flowView = MakeFlowView({LeaseId_});
    flowView->State->StartMutation();
    LeaseManager_->CheckLeases(flowView);
    RemoveJobs(flowView);
    LeaseManager_->TerminateStrayLeases(flowView);
    flowView->State->CommitMutation();
}

TEST_F(TChaosLeaseManagerTest, UnattachableStrayLeaseFailsTermination)
{
    // One attachment per check, and both rounds of the termination.
    EXPECT_CALL(*Client_, AttachChaosLease(LeaseId_, _))
        .Times(4)
        .WillRepeatedly(Return(MakeFuture<IPrerequisitePtr>(TransientError_)));

    {
        auto flowView = MakeFlowView({LeaseId_});
        flowView->State->StartMutation();
        LeaseManager_->CheckLeases(flowView);
        LeaseManager_->TerminateStrayLeases(flowView);
        flowView->State->CommitMutation();
    }

    {
        auto flowView = MakeFlowView({LeaseId_});
        flowView->State->StartMutation();
        LeaseManager_->CheckLeases(flowView);
        RemoveJobs(flowView);
        EXPECT_THROW(LeaseManager_->TerminateStrayLeases(flowView), std::exception);
    }
}

TEST_F(TChaosLeaseManagerTest, GoneStrayLeaseIsForgotten)
{
    EXPECT_CALL(*Client_, AttachChaosLease(LeaseId_, _))
        .WillOnce(Return(MakeFuture<IPrerequisitePtr>(TransientError_)))
        .WillOnce(Return(MakeFuture<IPrerequisitePtr>(TError(NYTree::EErrorCode::ResolveError, "No such chaos lease"))));

    auto flowView = MakeFlowView({LeaseId_});
    flowView->State->StartMutation();
    LeaseManager_->CheckLeases(flowView);
    RemoveJobs(flowView);
    LeaseManager_->TerminateStrayLeases(flowView);
    flowView->State->CommitMutation();
}

TEST_F(TChaosLeaseManagerTest, AttachedStrayLeaseIsKeptWhileAnotherIsUnattached)
{
    const auto attachedLeaseId = TLeaseId(TGuid::Create());
    ITransaction::TAbortedHandler aborted;
    auto attachedLease = MakeLease(attachedLeaseId, &aborted);

    EXPECT_CALL(*Client_, AttachChaosLease(attachedLeaseId, _))
        .WillOnce(Return(MakeFuture<IPrerequisitePtr>(attachedLease)));
    EXPECT_CALL(*Client_, AttachChaosLease(LeaseId_, _))
        .Times(3)
        .WillRepeatedly(Return(MakeFuture<IPrerequisitePtr>(TransientError_)));

    auto flowView = MakeFlowView({LeaseId_, attachedLeaseId});
    flowView->State->StartMutation();
    LeaseManager_->CheckLeases(flowView);
    RemoveJobs(flowView);
    EXPECT_THROW(LeaseManager_->TerminateStrayLeases(flowView), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NController
