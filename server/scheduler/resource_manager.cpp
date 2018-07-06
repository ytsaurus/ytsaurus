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
        // COMPAT(babenko)
        for (auto* resource : node->Resources().Load()) {
            auto& spec = resource->Spec();

            if (!spec->has_cpu() &&
                !spec->has_memory() &&
                !spec->has_disk())
            {
                switch (static_cast<EResourceKind>(spec->kind())) {
                    case EResourceKind::Cpu: {
                        auto* cpuSpec = spec->mutable_cpu();
                        cpuSpec->set_total_capacity(spec->total_capacity());
                        cpuSpec->set_cpu_to_vcpu_factor(1);
                        break;
                    }
                    case EResourceKind::Memory: {
                        auto* memorySpec = spec->mutable_memory();
                        memorySpec->set_total_capacity(spec->total_capacity());
                        break;
                    }
                    case EResourceKind::Disk: {
                        auto* diskSpec = spec->mutable_disk();
                        diskSpec->set_total_capacity(spec->total_capacity());
                        diskSpec->set_storage_class("hdd");
                        diskSpec->set_total_volume_slots(100);
                        diskSpec->set_device("/dev/xyz");
                        break;
                    }
                    default:
                        Y_UNREACHABLE();
                }
            }

            if (spec->has_cpu()) {
                resource->Kind() = EResourceKind::Cpu;
                spec->set_kind(NClient::NApi::NProto::RK_CPU);
            } else if (spec->has_memory()) {
                resource->Kind() = EResourceKind::Memory;
                spec->set_kind(NClient::NApi::NProto::RK_MEMORY);
            } else if (spec->has_disk()) {
                resource->Kind() = EResourceKind::Disk;
                spec->set_kind(NClient::NApi::NProto::RK_DISK);
            } else {
                THROW_ERROR_EXCEPTION("Resource %Qv is of an unrecogznied kind",
                    resource->GetId());
            }
        }

        TEnumIndexedVector<int, EResourceKind> counts{};
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
        auto* newNode = pod->Spec().Node().Load();
        auto* oldNode = pod->Spec().Node().LoadOld();

        if (newNode != oldNode) {
            FreePodResources(transaction, context, pod);
        }

        context->NetManager->UpdatePodAddresses(transaction, pod);

        if (newNode) {
            AllocatePodResources(transaction, context, pod);
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

    void AllocatePodResources(const TTransactionPtr& transaction, TResourceManagerContext* context, NObjects::TPod* pod)
    {
        auto* node = pod->Spec().Node().Load();

        if (context->InternetAddressManager) {
            context->InternetAddressManager->AssignInternetAddressesToPod(transaction, pod, node);
        }

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
            pod->Spec().Other().Load(),
            pod->Status().Other().Load(),
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
            THROW_ERROR_EXCEPTION("Cannot satisfy resource requests for pod %Qv at node %Qv",
                pod->GetId(),
                node->GetId())
                << errors;
        }

        UpdatePodDiskVolumeAllocations(
            pod->Status().Other()->mutable_disk_volume_allocations(),
            nativeResources,
            allocatorRequests,
            allocatorResponses);

        UpdateScheduledResourceAllocations(
            pod->GetId(),
            pod->Status().Other()->mutable_scheduled_resource_allocations(),
            nativeResources,
            allocatorResources,
            allocatorRequests,
            allocatorResponses);
    }

    void FreePodResources(const TTransactionPtr& transaction, TResourceManagerContext* context, NObjects::TPod* pod)
    {
        if (context->InternetAddressManager) {
            context->InternetAddressManager->RevokeInternetAddressesFromPod(transaction, pod);
        }

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
                        return allocation.pod_id() == pod->GetId();
                    }),
                scheduledAllocations->end());
        }

        scheduledResourceAllocations->Clear();

        pod->Status().Other()->mutable_disk_volume_allocations()->Clear();
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

} // namespace NScheduler
} // namespace NServer
} // namespace NYP

