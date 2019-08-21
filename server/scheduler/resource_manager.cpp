#include "resource_manager.h"
#include "helpers.h"

#include <yp/server/objects/node.h>
#include <yp/server/objects/resource.h>
#include <yp/server/objects/pod.h>
#include <yp/server/objects/transaction.h>

#include <yp/server/net/internet_address_manager.h>
#include <yp/server/net/net_manager.h>

#include <yp/server/master/bootstrap.h>

#include <yt/core/misc/indexed_vector.h>
#include <yt/core/misc/protobuf_helpers.h>

namespace NYP::NServer::NScheduler {

using namespace NServer::NMaster;
using namespace NServer::NObjects;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TResourceManager::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    void AssignPodToNode(
        const TTransactionPtr& transaction,
        TResourceManagerContext* context,
        NObjects::TNode* node,
        NObjects::TPod* pod)
    {
        node->Pods().Add(pod);
        UpdatePodSpec(transaction, pod, false);
        ReallocatePodResources(transaction, context, pod);
    }

    void RevokePodFromNode(
        const TTransactionPtr& transaction,
        TResourceManagerContext* context,
        NObjects::TPod* pod)
    {
        auto* node = pod->Spec().Node().Load();
        if (!node) {
            return;
        }

        node->Pods().Remove(pod);
        UpdatePodSpec(transaction, pod, false);
        ReallocatePodResources(transaction, context, pod);
    }

    void RemoveOrphanedAllocations(
        const NObjects::TTransactionPtr& transaction,
        NObjects::TNode* node)
    {
        for (auto* resource : node->Resources().Load()) {
            auto* scheduledAllocations = resource->Status().ScheduledAllocations().Get();
            scheduledAllocations->erase(
                std::remove_if(
                    scheduledAllocations->begin(),
                    scheduledAllocations->end(),
                    [&] (const auto& allocation) {
                        auto* pod = transaction->GetPod(allocation.pod_id());
                        if (!pod || !pod->DoesExist() || pod->MetaEtc().Load().uuid() != allocation.pod_uuid()) {
                            return true;
                        }
                        auto* podNode = pod->Spec().Node().Load();
                        return !podNode || podNode->GetId() != node->GetId();
                    }),
                scheduledAllocations->end());
        }
    }

    void PrepareUpdatePodSpec(
        const TTransactionPtr& transaction,
        NObjects::TPod* pod)
    {
        pod->Spec().Node().ScheduleLoad();

        transaction->ScheduleAllocateResources(pod);
    }

    void UpdatePodSpec(
        const TTransactionPtr& transaction,
        NObjects::TPod* pod,
        bool manual = true)
    {
        pod->Spec().UpdateTimestamp().Touch();

        UpdatePodAssignment(pod, manual);

        auto* oldNode = pod->Spec().Node().LoadOld();
        if (oldNode) {
            transaction->ScheduleNotifyAgent(oldNode);
        }

        auto* node = pod->Spec().Node().Load();
        if (node) {
            transaction->ScheduleNotifyAgent(node);
        }
    }

    void ValidateNodeResource(NObjects::TNode* node)
    {
        TEnumIndexedVector<EResourceKind, int> counts;
        for (auto* resource : node->Resources().Load()) {
            ++counts[resource->Kind().Load()];
        }

        for (auto kind : TEnumTraits<EResourceKind>::GetDomainValues()) {
            if (IsHomogeneous(kind) && counts[kind] > 1) {
                THROW_ERROR_EXCEPTION("More than one %Qlv resources assigned to node %Qv",
                    kind,
                    node->GetId());
            }
        }
    }

    void ReallocatePodResources(
        const TTransactionPtr& transaction,
        TResourceManagerContext* context,
        NObjects::TPod* pod)
    {
        const auto* newNode = pod->Spec().Node().Load();
        const auto* oldNode = pod->Spec().Node().LoadOld();

        if (newNode != oldNode) {
            FreePodResources(pod);
        }

        context->NetManager->UpdatePodAddresses(
            transaction,
            context->InternetAddressManager,
            pod);

        if (newNode) {
            AllocatePodResources(pod);
        }
    }

private:
    TBootstrap* const Bootstrap_;


    void UpdatePodAssignment(
        NObjects::TPod* pod,
        bool manual)
    {
        auto* node = pod->Spec().Node().Load();
        if (pod->Spec().Node().IsChanged()) {
            const auto* oldNode = pod->Spec().Node().LoadOld();
            if (node) {
                pod->Status().GenerationNumber() = pod->Status().GenerationNumber().Load() + 1;

                const auto& netManager = Bootstrap_->GetNetManager();
                pod->Status().Etc()->mutable_dns()->set_transient_fqdn(netManager->BuildTransientPodFqdn(pod));

                if (pod->GetState() != EObjectState::Created) {
                    if (manual) {
                        pod->UpdateSchedulingStatus(
                            ESchedulingState::Assigned,
                            Format("Pod is force-assigned to %Qv", node->GetId()),
                            node->GetId());
                    } else {
                        pod->UpdateSchedulingStatus(
                            ESchedulingState::Assigned,
                            Format("Pod is assigned to %Qv", node->GetId()),
                            node->GetId());
                    }
                }
            } else {
                pod->Status().Etc()->mutable_dns()->clear_transient_fqdn();

                if (pod->GetState() != EObjectState::Created) {
                    auto state = pod->Spec().EnableScheduling().Load()
                        ? ESchedulingState::Pending
                        : ESchedulingState::Disabled;
                    if (manual) {
                        pod->UpdateSchedulingStatus(
                            state,
                            Format("Pod is force-revoked from %Qv", oldNode->GetId()));
                    } else {
                        pod->UpdateSchedulingStatus(
                            state,
                            Format("Pod is revoked from %Qv and awaits scheduling", oldNode->GetId()));
                    }
                }
            }

            // NB! Overwrite eviction status even if there is no actual eviction
            // to prevent concurrent pod assignment / eviction status changes.
            pod->UpdateEvictionStatus(
                EEvictionState::None,
                EEvictionReason::None,
                "Eviction state reset due to pod assignment change");
        } else if (pod->Spec().EnableScheduling().IsChanged() &&
                   pod->Spec().EnableScheduling().Load() &&
                   !node &&
                   pod->GetState() != EObjectState::Created)
        {
            pod->UpdateSchedulingStatus(
                ESchedulingState::Pending,
                "Scheduling enabled; pod awaits scheduling");
        }
    }

    void AllocatePodResources(NObjects::TPod* pod)
    {
        auto* node = pod->Spec().Node().Load();

        TLocalResourceAllocator allocator;

        auto nativeResources = node->Resources().Load();
        for (auto* resource : nativeResources) {
            resource->Spec().ScheduleLoad();
            resource->Status().ScheduledAllocations().ScheduleLoad();
            resource->Status().ActualAllocations().ScheduleLoad();
        }

        std::vector<TLocalResourceAllocator::TResource> allocatorResources;
        allocatorResources.reserve(nativeResources.size());
        for (auto* resource : nativeResources) {
            allocatorResources.push_back(
                BuildAllocatorResource(
                    resource->GetId(),
                    resource->Spec().Load(),
                    resource->Status().ScheduledAllocations().Load(),
                    resource->Status().ActualAllocations().Load()));
        }

        auto allocatorRequests = BuildAllocatorResourceRequests(
            pod->GetId(),
            pod->Spec().Etc().Load(),
            pod->Status().Etc().Load(),
            allocatorResources);
        if (allocatorRequests.empty()) {
            return;
        }

        std::vector<TError> errors;
        std::vector<TLocalResourceAllocator::TResponse> allocatorResponses;
        if (!allocator.TryAllocate(
            pod->GetId(),
            allocatorRequests,
            allocatorResources,
            &allocatorResponses,
            &errors))
        {
            THROW_ERROR_EXCEPTION(
                NClient::NApi::EErrorCode::PodSchedulingFailure,
                "Cannot satisfy resource requests for pod %Qv at node %Qv",
                pod->GetId(),
                node->GetId())
                << errors;
        }

        UpdatePodDiskVolumeAllocations(
            pod->Status().Etc()->mutable_disk_volume_allocations(),
            allocatorRequests,
            allocatorResponses);

        UpdateScheduledResourceAllocations(
            pod->GetId(),
            pod->MetaEtc().Load().uuid(),
            pod->Status().Etc()->mutable_scheduled_resource_allocations(),
            nativeResources,
            allocatorResources,
            allocatorRequests,
            allocatorResponses);
    }

    void FreePodResources(NObjects::TPod* pod)
    {
        pod->Status().Etc()->mutable_scheduled_resource_allocations()->Clear();
        pod->Status().Etc()->mutable_disk_volume_allocations()->Clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

TResourceManager::TResourceManager(TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

void TResourceManager::AssignPodToNode(
    const TTransactionPtr& transaction,
    TResourceManagerContext* context,
    NObjects::TNode* node,
    NObjects::TPod* pod)
{
    Impl_->AssignPodToNode(transaction, context, node, pod);
}

void TResourceManager::RevokePodFromNode(
    const TTransactionPtr& transaction,
    TResourceManagerContext* context,
    NObjects::TPod* pod)
{
    Impl_->RevokePodFromNode(transaction, context, pod);
}

void TResourceManager::RemoveOrphanedAllocations(
    const NObjects::TTransactionPtr& transaction,
    NObjects::TNode* node)
{
    Impl_->RemoveOrphanedAllocations(transaction, node);
}

void TResourceManager::PrepareUpdatePodSpec(
    const TTransactionPtr& transaction,
    NObjects::TPod* pod)
{
    Impl_->PrepareUpdatePodSpec(transaction, pod);
}

void TResourceManager::UpdatePodSpec(
    const TTransactionPtr& transaction,
    NObjects::TPod* pod)
{
    Impl_->UpdatePodSpec(transaction, pod);
}

void TResourceManager::ValidateNodeResource(NObjects::TNode* node)
{
    Impl_->ValidateNodeResource(node);
}

void TResourceManager::ReallocatePodResources(
    const TTransactionPtr& transaction,
    TResourceManagerContext* context,
    NObjects::TPod* pod)
{
    Impl_->ReallocatePodResources(transaction, context, pod);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler

