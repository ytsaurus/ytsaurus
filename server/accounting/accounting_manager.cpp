#include "accounting_manager.h"
#include "config.h"
#include "helpers.h"
#include "private.h"

#include <yp/server/master/bootstrap.h>

#include <yp/server/scheduler/cluster.h>
#include <yp/server/scheduler/node.h>
#include <yp/server/scheduler/node_segment.h>
#include <yp/server/scheduler/account.h>
#include <yp/server/scheduler/pod.h>
#include <yp/server/scheduler/pod_set.h>
#include <yp/server/scheduler/helpers.h>

#include <yp/server/objects/transaction.h>
#include <yp/server/objects/transaction_manager.h>
#include <yp/server/objects/node_segment.h>
#include <yp/server/objects/account.h>
#include <yp/server/objects/pod.h>
#include <yp/server/objects/pod_set.h>
#include <yp/server/objects/helpers.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/small_set.h>

namespace NYP::NServer::NAccounting {

using namespace NScheduler;
using namespace NYT::NConcurrency;

using NObjects::GetObjectDisplayName;

////////////////////////////////////////////////////////////////////////////////

class TAccountingManager::TImpl
    : public TRefCounted
{
public:
    TImpl(
        NMaster::TBootstrap* bootstrap,
        TAccountingManagerConfigPtr config)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
    { }

    void Initialize()
    { }

    void PrepareValidateAccounting(NObjects::TPod* pod)
    {
        pod->Spec().Etc().ScheduleLoad();
        pod->Spec().Account().ScheduleLoad();
    }

    void ValidateAccounting(const std::vector<NObjects::TPod*>& pods)
    {
        YT_LOG_DEBUG("Starting accounting validation");

        THashMap<const NObjects::TAccount*, TResourceTotals> accountToUsageDelta;

        for (auto* pod : pods) {
            if (pod->DidExist()) {
                const auto* oldAccount = GetOldPodAccount(pod);
                const auto* oldSegment = GetOldPodSegment(pod);
                const auto& oldSpec = pod->Spec().Etc().LoadOld();
                if (oldSegment) {
                    accountToUsageDelta[oldAccount] -= ResourceUsageFromPodSpec(oldSpec, oldSegment->GetId());
                }
            }

            if (pod->DoesExist()) {
                const auto* newAccount = GetNewPodAccount(pod);
                const auto* newSegment = GetNewPodSegment(pod);
                const auto& newSpec = pod->Spec().Etc().Load();
                if (newSegment) {
                    accountToUsageDelta[newAccount] += ResourceUsageFromPodSpec(newSpec, newSegment->GetId());
                }
            }
        }

        for (const auto& pair : accountToUsageDelta) {
            auto* account = pair.first;
            const auto& usageDelta = pair.second;
            YT_LOG_DEBUG("Validating account usage increase (Account: %v, UsageDelta: %v)",
                account->GetId(),
                usageDelta);
            ValidateAccountUsageIncrease(account, usageDelta);
        }

        YT_LOG_DEBUG("Finished accounting validation");
    }

    void UpdateNodeSegmentsStatus(const TClusterPtr& cluster)
    {
        auto nodeSegments = cluster->GetNodeSegments();

        YT_LOG_DEBUG("Started committing node segments status update");

        try {
            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            auto transaction = WaitFor(transactionManager->StartReadWriteTransaction())
                .ValueOrThrow();

            std::vector<NObjects::TNodeSegment*> transactionNodeSegments;
            for (auto* nodeSegment : nodeSegments) {
                transactionNodeSegments.push_back(transaction->GetNodeSegment(nodeSegment->GetId()));
            }

            for (size_t index = 0; index < nodeSegments.size(); ++index) {
                auto* nodeSegment = nodeSegments[index];
                auto* transactionNodeSegment = transactionNodeSegments[index];
                if (!transactionNodeSegment->DoesExist()) {
                    continue;
                }

                auto computeTotals = [&] (auto* totals, const auto& nodes) {
                    ui64 totalCpuCapacity = 0;
                    ui64 totalMemoryCapacity = 0;
                    totals->mutable_disk_per_storage_class()->clear();
                    auto& storageClassToDiskTotals = *totals->mutable_disk_per_storage_class();
                    for (auto* node : nodes) {
                        totalCpuCapacity += GetCpuCapacity(node->CpuResource().GetTotalCapacities());
                        totalMemoryCapacity += GetMemoryCapacity(node->MemoryResource().GetTotalCapacities());
                        for (const auto& diskResource : node->DiskResources()) {
                            auto& diskTotals = storageClassToDiskTotals[diskResource.GetStorageClass()];
                            diskTotals.set_capacity(diskTotals.capacity() + GetDiskCapacity(
                                diskResource.GetTotalCapacities()));
                            diskTotals.set_bandwidth(diskTotals.bandwidth() + GetDiskBandwidth(
                                diskResource.GetTotalCapacities()));
                        }
                    }
                    totals->mutable_cpu()->set_capacity(totalCpuCapacity);
                    totals->mutable_memory()->set_capacity(totalMemoryCapacity);
                };

                auto* status = transactionNodeSegment->Status().Get();
                computeTotals(status->mutable_total_resources(), nodeSegment->Nodes());
                computeTotals(status->mutable_schedulable_resources(), nodeSegment->SchedulableNodes());
            }

            WaitFor(transaction->Commit())
                .ThrowOnError();

            YT_LOG_DEBUG("Node segments status update committed");
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Error committing node segments status update");
        }
    }

    void UpdateAccountsStatus(const TClusterPtr& cluster)
    {
        auto nodeSegments = cluster->GetNodeSegments();

        YT_LOG_DEBUG("Started committing accounts status update");

        try {
            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            auto transaction = WaitFor(transactionManager->StartReadWriteTransaction())
                .ValueOrThrow();

            auto accounts = cluster->GetAccounts();

            // Schedule load for all accounts.
            for (auto* account : accounts) {
                auto* transactionAccount = transaction->GetAccount(account->GetId());
                transactionAccount->Status().ScheduleLoad();
            }

            // Compute immediate usage.
            THashMap<TAccount*, NClient::NApi::NProto::TResourceTotals> accountToImmediateUsage;
            for (auto* account : accounts) {
                NClient::NApi::NProto::TResourceTotals usage;
                for (auto* pod : account->Pods()) {
                    auto* nodeSegment = pod->GetPodSet()->GetNodeSegment();
                    usage += ResourceUsageFromPodSpecRequests(
                        pod->ResourceRequests(),
                        pod->DiskVolumeRequests(),
                        pod->IP6AddressRequests(),
                        nodeSegment->GetId());
                }
                accountToImmediateUsage[account] = std::move(usage);
            }

            // Compute recursive usage.
            THashMap<TAccount*, NClient::NApi::NProto::TResourceTotals> accountToUsage;
            for (auto* account : accounts) {
                if (!account->GetParent()) {
                    ComputeRecursiveAccountUsage(
                        account,
                        &accountToUsage,
                        accountToImmediateUsage);
                }
            }

            // Update statuses.
            for (auto* account : accounts) {
                auto* transactionAccount = transaction->GetAccount(account->GetId());
                if (!transactionAccount->DoesExist()) {
                    continue;
                }
                *transactionAccount->Status()->mutable_immediate_resource_usage() = std::move(accountToImmediateUsage[account]);
                *transactionAccount->Status()->mutable_resource_usage() = std::move(accountToUsage[account]);
            }

            WaitFor(transaction->Commit())
                .ThrowOnError();

            YT_LOG_DEBUG("Accounts status update committed");
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Error committing accounts status update");
        }
    }

private:
    NMaster::TBootstrap* const Bootstrap_;
    const TAccountingManagerConfigPtr Config_;

    DECLARE_THREAD_AFFINITY_SLOT(SchedulerThread);


    NClient::NApi::NProto::TResourceTotals ComputeRecursiveAccountUsage(
        TAccount* currentAccount,
        THashMap<TAccount*, NClient::NApi::NProto::TResourceTotals>* accountToUsage,
        const THashMap<TAccount*, NClient::NApi::NProto::TResourceTotals>& accountToImmediateUsage)
    {
        auto pair = accountToUsage->emplace(currentAccount, accountToImmediateUsage.at(currentAccount));
        if (!pair.second) {
            YT_LOG_WARNING(
                "Account visited at least twice during recursive traversal; "
                "this indicates cyclic dependencies in accounts hierarchy (AccountId: %v)",
                currentAccount->GetId());
            return {};
        }

        auto& currentUsage = pair.first->second;
        for (auto* childAccount : currentAccount->Children()) {
            currentUsage += ComputeRecursiveAccountUsage(
                childAccount,
                accountToUsage,
                accountToImmediateUsage);
        }

        return currentUsage;
    }

    void ValidateAccountUsageIncrease(const NObjects::TAccount* account, const TResourceTotals& usageDelta)
    {
        SmallSet<const NObjects::TAccount*, 16> visitedAccounts;
        const auto* currentAccount = account;
        while (currentAccount) {
            if (!visitedAccounts.insert(currentAccount).second) {
                THROW_ERROR_EXCEPTION("Cyclic dependencies found while checking limits of account %v",
                    GetObjectDisplayName(account));
            }

            auto usage = account->Status().Load().resource_usage() + usageDelta;
            const auto& limits = account->Spec().Etc().Load().resource_limits();

            for (const auto& perSegmentPair : usage.per_segment()) {
                const auto& segmentId = perSegmentPair.first;

                auto getPerSegmentTotals = [&] (const TResourceTotals& totals) -> const TPerSegmentResourceTotals& {
                    auto it = totals.per_segment().find(segmentId);
                    static const TPerSegmentResourceTotals Default;
                    return it == totals.per_segment().end() ? Default : it->second;
                };

                const auto& deltaPerSegment = getPerSegmentTotals(usageDelta);
                const auto& usagePerSegment = getPerSegmentTotals(usage);
                const auto& limitsPerSegment = getPerSegmentTotals(limits);

                if (deltaPerSegment.cpu().capacity() > 0 && usagePerSegment.cpu().capacity() > limitsPerSegment.cpu().capacity()) {
                    THROW_ERROR_EXCEPTION(
                        NClient::NApi::EErrorCode::AccountLimitExceeded,
                        "Account %v is over CPU limit in segment %Qv",
                        GetObjectDisplayName(currentAccount),
                        segmentId)
                        << TErrorAttribute("usage", usagePerSegment.cpu().capacity())
                        << TErrorAttribute("limit", limitsPerSegment.cpu().capacity());
                }

                if (deltaPerSegment.memory().capacity() > 0 && usagePerSegment.memory().capacity() > limitsPerSegment.memory().capacity()) {
                    THROW_ERROR_EXCEPTION(
                        NClient::NApi::EErrorCode::AccountLimitExceeded,
                        "Account %v is over memory limit in segment %Qv",
                        GetObjectDisplayName(currentAccount),
                        segmentId)
                        << TErrorAttribute("usage", usagePerSegment.memory().capacity())
                        << TErrorAttribute("limit", limitsPerSegment.memory().capacity());
                }

                // NB! Missing internet address limit corresponds to the infinite limit for the backward compatibility.
                if (limitsPerSegment.has_internet_address() && deltaPerSegment.internet_address().capacity() > 0 && usagePerSegment.internet_address().capacity() > limitsPerSegment.internet_address().capacity()) {
                    THROW_ERROR_EXCEPTION(
                        NClient::NApi::EErrorCode::AccountLimitExceeded,
                        "Account %v is over internet address limit in segment %Qv",
                        GetObjectDisplayName(currentAccount),
                        segmentId)
                        << TErrorAttribute("usage", usagePerSegment.internet_address().capacity())
                        << TErrorAttribute("limit", limitsPerSegment.internet_address().capacity());
                }

                for (const auto& perStorageClassPair : usagePerSegment.disk_per_storage_class()) {
                    const auto& storageClass = perStorageClassPair.first;

                    auto getPerStorageClassTotals = [&] (const TPerSegmentResourceTotals& totals) -> const NClient::NApi::NProto::TPerSegmentResourceTotals_TDiskTotals& {
                        auto it = totals.disk_per_storage_class().find(storageClass);
                        static const NClient::NApi::NProto::TPerSegmentResourceTotals_TDiskTotals Default;
                        return it == totals.disk_per_storage_class().end() ? Default : it->second;
                    };

                    const auto& deltaPerStorageClass = getPerStorageClassTotals(deltaPerSegment);
                    const auto& usagePerStorageClass = getPerStorageClassTotals(usagePerSegment);
                    const auto& limitsPerStorageClass = getPerStorageClassTotals(limitsPerSegment);

                    if (deltaPerStorageClass.capacity() > 0 && usagePerStorageClass.capacity() > limitsPerStorageClass.capacity()) {
                        THROW_ERROR_EXCEPTION(
                            NClient::NApi::EErrorCode::AccountLimitExceeded,
                            "Account %v is over disk capacity limit in segment %Qv for storage class %Qv",
                            GetObjectDisplayName(currentAccount),
                            segmentId,
                            storageClass)
                            << TErrorAttribute("usage", usagePerStorageClass.capacity())
                            << TErrorAttribute("limit", limitsPerStorageClass.capacity());
                    }

                    if (deltaPerStorageClass.bandwidth() > 0 && usagePerStorageClass.bandwidth() > limitsPerStorageClass.bandwidth()) {
                        THROW_ERROR_EXCEPTION(
                            NClient::NApi::EErrorCode::AccountLimitExceeded,
                            "Account %v is over disk bandwidth limit in segment %Qv for storage class %Qv",
                            GetObjectDisplayName(currentAccount),
                            segmentId,
                            storageClass)
                            << TErrorAttribute("usage", usagePerStorageClass.bandwidth())
                            << TErrorAttribute("limit", limitsPerStorageClass.bandwidth());
                    }
                }
            }

            currentAccount = currentAccount->Spec().Parent().Load();
        }
    }


    static NObjects::TAccount* GetOldPodAccount(NObjects::TPod* pod)
    {
        auto* podAccount = pod->Spec().Account().LoadOld();
        if (podAccount) {
            return podAccount;
        }
        return pod->PodSet().Load()->Spec().Account().LoadOld();
    }

    static NObjects::TAccount* GetNewPodAccount(NObjects::TPod* pod)
    {
        auto* podAccount = pod->Spec().Account().Load();
        if (podAccount) {
            return podAccount;
        }
        return pod->PodSet().Load()->Spec().Account().Load();
    }


    static NObjects::TNodeSegment* GetOldPodSegment(NObjects::TPod* pod)
    {
        return pod->PodSet().Load()->Spec().NodeSegment().LoadOld();
    }

    static NObjects::TNodeSegment* GetNewPodSegment(NObjects::TPod* pod)
    {
        return pod->PodSet().Load()->Spec().NodeSegment().Load();
    }
};

////////////////////////////////////////////////////////////////////////////////

TAccountingManager::TAccountingManager(
    NMaster::TBootstrap* bootstrap,
    TAccountingManagerConfigPtr config)
    : Impl_(New<TImpl>(bootstrap, std::move(config)))
{ }

void TAccountingManager::Initialize()
{
    Impl_->Initialize();
}

void TAccountingManager::PrepareValidateAccounting(NObjects::TPod* pod)
{
    Impl_->PrepareValidateAccounting(pod);
}

void TAccountingManager::ValidateAccounting(const std::vector<NObjects::TPod*>& pods)
{
    Impl_->ValidateAccounting(pods);
}

void TAccountingManager::UpdateNodeSegmentsStatus(const TClusterPtr& cluster)
{
    Impl_->UpdateNodeSegmentsStatus(cluster);
}

void TAccountingManager::UpdateAccountsStatus(const TClusterPtr& cluster)
{
    Impl_->UpdateAccountsStatus(cluster);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NAccounting

