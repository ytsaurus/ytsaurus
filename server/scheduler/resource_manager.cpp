#include "resource_manager.h"

#include "helpers.h"

#include <yp/server/objects/node.h>
#include <yp/server/objects/pod.h>
#include <yp/server/objects/resource.h>
#include <yp/server/objects/persistent_disk.h>
#include <yp/server/objects/persistent_volume_claim.h>
#include <yp/server/objects/persistent_volume.h>
#include <yp/server/objects/transaction.h>

#include <yp/server/net/internet_address_manager.h>
#include <yp/server/net/net_manager.h>

#include <yp/server/master/bootstrap.h>

#include <yp/server/lib/cluster/allocation_statistics.h>

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
        node->Status().Pods().Add(pod);
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

        node->Status().Pods().Remove(pod);
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
            if (IsSingletonResource(kind) && counts[kind] > 1) {
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
            AllocatePodResources(transaction, pod);
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

            pod->ResetAgentStatus();

            pod->UpdateMaintenanceStatus(
                EPodMaintenanceState::None,
                "Maintenance state reset due to pod assignment change",
                /* infoUpdate */ TGenericClearUpdate());

            pod->Status().Etc()->clear_node_alerts();

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

    void AllocatePodResources(
        const TTransactionPtr& transaction,
        NObjects::TPod* pod)
    {
        ValidatePodVolumeClaims(transaction, pod);

        TLocalResourceAllocator allocator;

        auto* node = pod->Spec().Node().Load();
        auto nativeResources = node->Resources().Load();
        for (auto* resource : nativeResources) {
            resource->Spec().ScheduleLoad();
            resource->Status().ScheduledAllocations().ScheduleLoad();
            resource->Status().ActualAllocations().ScheduleLoad();
        }

        // NB: allocatorResources[i] should correspond to nativeResources[i]. Never reorder it!
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

        UpdatePodDiskVolumeMounts(
            transaction,
            pod);

        UpdatePodGpuAllocations(
            pod->Status().Etc()->mutable_gpu_allocations(),
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

    void ValidatePodVolumeClaims(
        const TTransactionPtr& transaction,
        NObjects::TPod* pod)
    {
        const auto& podSpecEtc = pod->Spec().Etc().Load();
        TNode* persistentVolumeNode = nullptr;
        for (const auto& protoClaim : podSpecEtc.disk_volume_claims()) {
            const auto& claimId = protoClaim.claim_id();
            auto* claim = transaction->GetPersistentVolumeClaim(claimId);
            if (!claim->DoesExist()) {
                THROW_ERROR_EXCEPTION(
                    NClient::NApi::EErrorCode::PodSchedulingFailure,
                    "Pod %Qv refers to non-existing persistent volume claim %Qv",
                    pod->GetId(),
                    claimId);
            }

            if (!claim->Spec().Etc().Load().has_existing_volume_policy()) {
                // TODO(babenko)
                THROW_ERROR_EXCEPTION(
                    NClient::NApi::EErrorCode::PodSchedulingFailure,
                    "Pod %Qv refers to persistent volume claim %Qv of unsupported policy",
                    pod->GetId(),
                    claimId);
            }

            auto* volume = claim->Status().BoundVolume().Load();
            if (!volume) {
                // TODO(babenko)
                THROW_ERROR_EXCEPTION(
                    NClient::NApi::EErrorCode::PodSchedulingFailure,
                    "Pod %Qv refers to persistent volume claim %Qv not bound to any volume",
                    pod->GetId(),
                    claimId);
            }

            auto* disk = volume->Disk().Load();
            auto* diskNode = disk->Status().AttachedToNode().Load();
            if (!diskNode) {
                THROW_ERROR_EXCEPTION(
                    NClient::NApi::EErrorCode::PodSchedulingFailure,
                    "Pod %Qv refers to persistent volume claim %Qv that is bound to volume %Qv belonging to disk %Qv not attached to any node",
                    pod->GetId(),
                    claimId,
                    volume->GetId(),
                    disk->GetId());
            }

            if (persistentVolumeNode && persistentVolumeNode != diskNode) {
                THROW_ERROR_EXCEPTION(
                    NClient::NApi::EErrorCode::PodSchedulingFailure,
                    "Pod %Qv refers to persistent volumes belonging to distinct nodes %Qv and %Qv",
                    pod->GetId(),
                    persistentVolumeNode->GetId(),
                    diskNode->GetId());
            }

            persistentVolumeNode = diskNode;
        }

        auto* node = pod->Spec().Node().Load();
        if (persistentVolumeNode && node != persistentVolumeNode) {
            THROW_ERROR_EXCEPTION(
                NClient::NApi::EErrorCode::PodSchedulingFailure,
                "Pod %Qv cannot be placed at node %Qv since it refers to persistent volume claims bound to volume(s) at node %Qv",
                pod->GetId(),
                node->GetId(),
                persistentVolumeNode->GetId());
        }
    }

    void UpdatePodDiskVolumeMounts(
        const TTransactionPtr& transaction,
        NObjects::TPod* pod)
    {
        const auto& podSpecEtc = pod->Spec().Etc().Load();
        auto* podStatusEtc = pod->Status().Etc().Get();
        podStatusEtc->mutable_disk_volume_mounts()->Clear();
        for (const auto& protoClaim : podSpecEtc.disk_volume_claims()) {
            auto* claim = transaction->GetPersistentVolumeClaim(protoClaim.claim_id());
            auto* volume = claim->Status().BoundVolume().Load();
            auto* protoMount = podStatusEtc->add_disk_volume_mounts();
            protoMount->set_volume_id(volume->GetId());
            protoMount->set_name(protoClaim.name());
            protoMount->mutable_labels()->CopyFrom(protoClaim.labels());
            YT_VERIFY(volume);
            pod->Status().MountedPersistentVolumes().Add(volume);
        }
    }

    void FreePodResources(NObjects::TPod* pod)
    {
        pod->Status().Etc()->mutable_scheduled_resource_allocations()->Clear();
        pod->Status().Etc()->mutable_disk_volume_allocations()->Clear();
        pod->Status().Etc()->mutable_disk_volume_mounts()->Clear();
        pod->Status().Etc()->mutable_gpu_allocations()->Clear();
        pod->Status().MountedPersistentVolumes().Clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

TResourceManager::TResourceManager(TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

TResourceManager::~TResourceManager()
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

