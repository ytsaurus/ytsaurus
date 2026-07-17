#include "private.h"

#include "config.h"
#include "lease_manager.h"
#include "yt_connector.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/misc/jitter.h>

#include <library/cpp/containers/concurrent_hash_set/concurrent_hash_set.h>

#include <util/random/random.h>

namespace NYT::NFlow::NController {

using namespace NYT::NApi;
using namespace NYT::NConcurrency;
using namespace NYT::NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ControllerLogger;

////////////////////////////////////////////////////////////////////////////////

class TLeaseManager
    : public ILeaseManager
{
public:
    TLeaseManager(
        IYTConnectorPtr connector,
        TLeaseManagerConfigPtr config)
        : Connector_(std::move(connector))
        , Config_(std::move(config))
    { }

    void TerminateStrayLeases(const TFlowViewPtr& flowView) override
    {
        THashSet<TLeaseId> knownLeases;
        auto& layout = flowView->State->ExecutionSpec->Layout;
        for (const auto& [jobId, job] : layout->Jobs) {
            knownLeases.insert(job->LeaseId);
        }

        std::vector<TLeaseId> leasesToTerminate;
        for (const auto& [leaseId, transaction] : Leases_) {
            if (knownLeases.contains(leaseId) || ExpiredLeases_.Contains(leaseId)) {
                continue;
            }
            leasesToTerminate.push_back(leaseId);
        }

        std::vector<TLeaseId> terminatingLeases;
        ui64 terminatedLeases = 0;
        std::vector<TFuture<void>> abortFutures;

        auto flush = [&] () {
            WaitFor(AllSucceeded(abortFutures))
                .ThrowOnError();
            abortFutures = {};
            for (auto leaseId : terminatingLeases) {
                UnregisterLease(leaseId);
            }
            terminatedLeases += terminatingLeases.size();
            terminatingLeases = {};
        };

        for (const auto& leaseId : leasesToTerminate) {
            terminatingLeases.push_back(leaseId);
            abortFutures.emplace_back(GetOrCrash(Leases_, leaseId)->Abort());
            if (std::ssize(abortFutures) >= Config_->MaxConcurrentRequests) {
                flush();
            }
        }

        flush();

        YT_TLOG_INFO("Terminated leases")
            .With("LeaseCount", terminatedLeases);
    }

    void CheckLeases(const TFlowViewPtr& flowView) override
    {
        const auto& layout = flowView->State->ExecutionSpec->Layout;
        ui64 attachedLeases = 0;
        ui64 totalLeases = 0;

        std::vector<TJobPtr> expiredLeaseJobs;

        for (const auto& [jobId, job] : layout->Jobs) {
            if (job->LeaseId == NullLeaseId) {
                continue;
            }
            totalLeases += 1;

            if (!Leases_.contains(job->LeaseId)) {
                NApi::TTransactionAttachOptions options;
                options.PingPeriod = Config_->LeasePingPeriod;
                auto transaction = Connector_->GetClient()->AttachTransaction(job->LeaseId, options);
                RegisterLease(transaction);
                attachedLeases += 1;
            }
            if (ExpiredLeases_.Contains(job->LeaseId)) {
                expiredLeaseJobs.push_back(job);
            }
        }

        for (const auto& job : expiredLeaseJobs) {
            layout->RemoveJob(job->JobId, EJobFinishReason::ExpiredLease);

            auto partition = GetOrCrash(layout->Partitions, job->PartitionId);
            auto error = TError("Job is lost, lease is expired (JobId: %v, PartitionId: %v, ComputationId: %v, LeaseId: %v)",
                job->JobId,
                job->PartitionId,
                partition->ComputationId,
                job->LeaseId);
            YT_TLOG_EVENT_FLUENT(PublicControllerLogger, NLogging::ELogLevel::Error, "")
                .With(error);

            auto partitionState = flowView->EphemeralState->GetPartitionState(job->PartitionId);
            partitionState->PreviousJobFailInstant = TInstant::Seconds(flowView->State->CurrentTimestamp.Underlying());
            partitionState->PreviousJobFailError = std::move(error);
        }

        YT_TLOG_INFO("Check leases")
            .With("Attached", attachedLeases)
            .With("Expired", expiredLeaseJobs.size())
            .With("Total", totalLeases);
    }

    void PrepareLeases(const TFlowViewPtr& flowView) override
    {
        const auto& layout = flowView->State->ExecutionSpec->Layout;

        struct TCreateJobLease
        {
            TJobId JobId;
            TLeaseId LeaseId;
        };

        ssize_t createdLeases = 0;

        std::vector<TFuture<TCreateJobLease>> futures;
        auto flush = [&] () {
            auto results = WaitFor(AllSucceeded(futures)).ValueOrThrow();
            for (auto& result : results) {
                layout->UpdateJob(result.JobId, result.LeaseId);
                createdLeases++;
            }
            futures = {};
        };

        for (const auto& [jobId, job] : layout->Jobs) {
            if (job->LeaseId != NullLeaseId) {
                continue;
            }

            auto processTransaction = BIND([weakThis = MakeWeak(this), jobId] (const ITransactionPtr& transaction) {
                if (auto strongThis = weakThis.Lock()) {
                    strongThis->RegisterLease(transaction);
                    return TCreateJobLease{jobId, transaction->GetId()};
                }
                THROW_ERROR_EXCEPTION("Lease manager is dead");
            });

            NApi::TTransactionStartOptions options;
            options.Timeout = Config_->LeaseTimeout;
            options.PingPeriod = Config_->LeasePingPeriod;
            auto attributes = NYTree::CreateEphemeralAttributes();
            attributes->Set("title", Format("Flow: lease for job %v", jobId));
            options.Attributes = std::move(attributes);
            auto future = Connector_->GetClient()->StartTransaction(ETransactionType::Master, options).Apply(processTransaction);

            futures.push_back(std::move(future));
            if (std::ssize(futures) >= Config_->MaxConcurrentRequests) {
                flush();
            }
        }

        flush();
        YT_TLOG_INFO("Prepared leases")
            .With("LeaseCount", createdLeases);
    }

private:
    const IYTConnectorPtr Connector_;
    const TLeaseManagerConfigPtr Config_;

    THashMap<TLeaseId, ITransactionPtr> Leases_;
    TConcurrentHashSet<TLeaseId> ExpiredLeases_;

    void RegisterLease(ITransactionPtr transaction)
    {
        transaction->SubscribeAborted(BIND(&TLeaseManager::AbortLease, MakeWeak(this), transaction->GetId()));
        Leases_[transaction->GetId()] = transaction;
    }

    void UnregisterLease(const TLeaseId& leaseId)
    {
        Leases_.erase(leaseId);
        YT_VERIFY(ExpiredLeases_.Erase(leaseId));
    }

    void AbortLease(const TLeaseId& leaseId, const TError& /*error*/)
    {
        ExpiredLeases_.Insert(leaseId);
    }
};

ILeaseManagerPtr CreateLeaseManager(
    IYTConnectorPtr connector,
    TLeaseManagerConfigPtr config)
{
    return New<TLeaseManager>(std::move(connector), std::move(config));
}

} // namespace NYT::NFlow::NController
