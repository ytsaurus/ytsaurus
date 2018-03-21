#include "resource_manager.h"
#include "helpers.h"

#include <yp/server/objects/node.h>
#include <yp/server/objects/resource.h>
#include <yp/server/objects/pod.h>
#include <yp/server/objects/transaction.h>

#include <yp/server/net/net_manager.h>

#include <yp/server/master/bootstrap.h>

#include <yt/core/misc/indexed_vector.h>
#include <yt/core/misc/protobuf_helpers.h>

namespace NYP {
namespace NServer {
namespace NScheduler {

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
        NObjects::TNode* node,
        NObjects::TPod* pod)
    {
        node->Pods().Add(pod);
        UpdatePodSpec(transaction, pod, false);
        ReallocatePodResources(transaction, pod);
    }

    void RevokePodFromNode(
        const TTransactionPtr& transaction,
        NObjects::TPod* pod)
    {
        auto* node = pod->Spec().Node().Load();
        if (!node) {
            return;
        }

        node->Pods().Remove(pod);
        UpdatePodSpec(transaction, pod, false);
        ReallocatePodResources(transaction, pod);
    }

    void PrepareUpdatePodSpec(
        const TTransactionPtr& transaction,
        NObjects::TPod* pod)
    {
        pod->Spec().Node().ScheduleLoad();

        transaction->ScheduleAllocateResources(pod);
        transaction->ScheduleUpdatePodAddresses(pod);
    }

    void UpdatePodSpec(
        const TTransactionPtr& transaction,
        NObjects::TPod* pod,
        bool manual = true)
    {
        pod->Spec().UpdateTimestamp().Touch();

        UpdatePodAssignment(transaction, pod, manual);

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
        TEnumIndexedVector<int, EResourceKind> counts{};
        for (auto* resource : node->Resources().Load()) {
            ++counts[EResourceKind(resource->Spec().Load().kind())];
        }

        for (auto kind : TEnumTraits<EResourceKind>::GetDomainValues()) {
            if (IsHomogeneous(kind) && counts[kind] > 1) {
                THROW_ERROR_EXCEPTION("More than one %Qlv resources assigned to node %Qv",
                    kind,
                    node->GetId());
            }
        }
    }

    void ReallocatePodResources(const TTransactionPtr& transaction, NObjects::TPod* pod)
    {
        auto* newNode = pod->Spec().Node().Load();
        auto* oldNode = pod->Spec().Node().LoadOld();

        if (newNode != oldNode) {
            FreePodResources(transaction, pod);
        }

        if (newNode) {
            AllocatePodResources(pod);
        }
    }

private:
    TBootstrap* const Bootstrap_;


    void UpdatePodAssignment(
        const TTransactionPtr& transaction,
        NObjects::TPod* pod,
        bool manual)
    {
        auto* node = pod->Spec().Node().Load();
        if (pod->Spec().Node().IsChanged()) {
            const auto* oldNode = pod->Spec().Node().LoadOld();
            if (node) {
                pod->Status().GenerationNumber() = pod->Status().GenerationNumber().Load() + 1;

                const auto& netManager = Bootstrap_->GetNetManager();
                pod->Status().Other()->mutable_dns()->set_transient_fqdn(netManager->BuildTransientPodFqdn(pod));

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
                pod->Status().Other()->mutable_dns()->clear_transient_fqdn();

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

            if (pod->Status().Other().Load().eviction().state() != NClient::NApi::NProto::ES_NONE) {
                pod->UpdateEvictionStatus(
                    EEvictionState::None,
                    EEvictionReason::None,
                    "Eviction state reset due to pod assignment change");
            }
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

        auto requests  = allocator.BuildRequests(pod->Spec().Other().Load().resource_requests());
        if (requests.empty()) {
            return;
        }

        auto nativeResources = node->Resources().Load();
        for (auto* resource : nativeResources) {
            resource->Spec().ScheduleLoad();
            resource->Status().ScheduledAllocations().ScheduleLoad();
            resource->Status().ActualAllocations().ScheduleLoad();
        }

        std::vector<TLocalResourceAllocator::TResource> resources;
        for (auto* resource : nativeResources) {
            resources.push_back({
                resource->GetId(),
                &resource->Spec().Load(),
                &resource->Status().ScheduledAllocations().Load(),
                &resource->Status().ActualAllocations().Load()
            });
        }

        std::vector<TLocalResourceAllocator::TAllocation> allocations;

        std::vector<TError> errors;
        if (!allocator.ComputeAllocations(
            pod->GetId(),
            requests,
            resources,
            &allocations,
            &errors))
        {
            THROW_ERROR_EXCEPTION("Cannot satisfy resource requests for pod %Qv at node %Qv",
                pod->GetId(),
                node->GetId())
                << errors;
        }

        auto* scheduledPodAllocations = pod->Status().Other()->mutable_scheduled_resource_allocations();
        scheduledPodAllocations->Clear();

        YCHECK(resources.size() == allocations.size());
        for (auto index = 0; index < allocations.size(); ++index) {
            auto* resource = nativeResources[index];
            auto* scheduledResourceAllocations = resource->Status().ScheduledAllocations().Get();
            auto& newAllocations = allocations[index].NewAllocations;
            
            scheduledResourceAllocations->erase(
                std::remove_if(
                    scheduledResourceAllocations->begin(),
                    scheduledResourceAllocations->end(),
                    [&] (const auto& currentAllocation) {
                        return FromProto<TObjectId>(currentAllocation.pod_id()) == pod->GetId();
                    }),
                scheduledResourceAllocations->end());

            for (auto& newAllocation : newAllocations) {
                scheduledResourceAllocations->emplace_back();
                scheduledResourceAllocations->back().Swap(&newAllocation);
            }

            if (!newAllocations.empty()) {
                auto* protoAllocation = scheduledPodAllocations->Add();
                ToProto(protoAllocation->mutable_resource_id(), resource->GetId());
            }
        }
    }

    void FreePodResources(const TTransactionPtr& transaction, NObjects::TPod* pod)
    {
        auto* scheduledResourceAllocations = pod->Status().Other()->mutable_scheduled_resource_allocations();

        std::vector<TResource*> resources;
        for (const auto& allocation : *scheduledResourceAllocations) {
            auto resourceId = FromProto<TObjectId>(allocation.resource_id());
            auto* resource = transaction->GetResource(resourceId);
            resource->Status().ScheduledAllocations().ScheduleLoad();
            resources.push_back(resource);
        }

        for (auto* resource : resources) {
            auto* scheduledAllocations = resource->Status().ScheduledAllocations().Get();
            scheduledAllocations->erase(
                std::remove_if(
                    scheduledAllocations->begin(),
                    scheduledAllocations->end(),
                    [&] (const auto& allocation) {
                        return FromProto<TObjectId>(allocation.pod_id()) == pod->GetId();
                    }),
                scheduledAllocations->end());
        }

        scheduledResourceAllocations->Clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

TResourceManager::TResourceManager(TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

void TResourceManager::AssignPodToNode(
    const TTransactionPtr& transaction,
    NObjects::TNode* node,
    NObjects::TPod* pod)
{
    Impl_->AssignPodToNode(transaction, node, pod);
}

void TResourceManager::RevokePodFromNode(
    const TTransactionPtr& transaction,
    NObjects::TPod* pod)
{
    Impl_->RevokePodFromNode(transaction, pod);
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
    NObjects::TPod* pod)
{
    Impl_->ReallocatePodResources(transaction, pod);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP

