#include "node_tracker.h"
#include "porto.h"
#include "helpers.h"
#include "private.h"

#include <yp/server/objects/transaction.h>
#include <yp/server/objects/node.h>
#include <yp/server/objects/pod.h>
#include <yp/server/objects/pod_set.h>
#include <yp/server/objects/resource.h>
#include <yp/server/objects/persistent_disk.h>
#include <yp/server/objects/persistent_volume.h>

#include <yp/server/master/config.h>
#include <yp/server/master/bootstrap.h>

#include <yp/server/scheduler/helpers.h>

#include <yp/client/nodes/node_tracker_service_proxy.h>
#include <yp/client/nodes/agent_service_proxy.h>

#include <yt/ytlib/auth/secret_vault_service.h>

#include <yt/core/rpc/caching_channel_factory.h>

#include <yt/core/rpc/grpc/channel.h>

#include <yt/core/concurrency/throughput_throttler.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <queue>

#include <contrib/libs/protobuf/util/time_util.h>

namespace NYP::NServer::NNodes {

using namespace NServer::NObjects;
using namespace NServer::NMaster;
using namespace NClient::NNodes;
using namespace NClient::NNodes::NProto;
using namespace NClient::NApi::NProto;
using namespace NYT::NRpc;
using namespace NYT::NRpc::NGrpc;
using namespace NYT::NConcurrency;
using namespace NYT::NAuth;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto AgentNotificationTick = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

class TNodeTracker::TImpl
    : public TRefCounted
{
public:
    TImpl(TBootstrap* bootstrap, TNodeTrackerConfigPtr config)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
        , AgentNotificationExecutor_(New<TPeriodicExecutor>(
            bootstrap->GetControlInvoker(),
            BIND(&TImpl::OnAgentNotificationTick, MakeWeak(this)),
            AgentNotificationTick))
        , AgentNotificationThrottler_(CreateReconfigurableThroughputThrottler(
            Config_->AgentNotificationThrottler,
            {},
            Profiler.AppendPath("/agent_notification_throttler")))
    {
        AgentNotificationExecutor_->Start();
    }

    TNode* ProcessHandshake(
        const TTransactionPtr& transaction,
        const TString& nodeId,
        const TString& address,
        const TString& version)
    {
        auto* Node_ = transaction->GetNode(nodeId);
        Node_->ValidateExists();

        auto now = TInstant::Now();
        Node_->Status().AgentAddress() = address;
        Node_->Status().LastSeenTime() = now;
        Node_->Status().EpochId() = TEpochId::Create();
        Node_->Status().HeartbeatSequenceNumber() = 0;
        Node_->Status().Etc()->set_agent_version(version);
        Node_->Status().Etc()->set_last_handshake_time(now.MicroSeconds());
        Node_->Status().Etc()->set_last_handshake_master_address(Bootstrap_->GetFqdn());

        YT_LOG_DEBUG("Handshake received (NodeId: %v, Address: %v, Version: %v, EpochId: %v)",
            nodeId,
            address,
            version,
            Node_->Status().EpochId().Load());

        return Node_;
    }

    class THeartbeatHandler
    {
    public:
        THeartbeatHandler(
            TImpl* owner,
            TTransactionPtr transaction,
            TNode* node,
            TEpochId epochId,
            ui64 sequenceNumber,
            const TReqHeartbeat* request,
            TRspHeartbeat* response)
            : Owner_(owner)
            , Transaction_(std::move(transaction))
            , Node_(node)
            , EpochId_(epochId)
            , SequenceNumber_(sequenceNumber)
            , Request_(request)
            , Response_(response)
            , Logger(NLogging::TLogger(Owner_->Logger)
                .AddTag("NodeId: %v",
                    Node_->GetId()))
        { }

        void Run()
        {
            SchedulePreloadNode();

            ValidateSequencing();

            YT_LOG_DEBUG("Heartbeat received (EpochId: %v, SequenceNumber: %v)",
                EpochId_,
                SequenceNumber_);

            UpdateNode();

            if (!CheckUnknownPods()) {
                return;
            }

            if (!CheckUnknownPersistentVolumes()) {
                return;
            }

            ProcessReportedPods();

            SchedulePodInstall();

            ProcessReportedPersistentDisks();

            ProcessReportedPersistentVolumes();

            AnalyzePodInstallErrors();

            RequestSecrets();

            for (auto& [pod, podSpec] : PodsToUpdate_) {
                SchedulePodDynamicAttributesLoad(pod);
            }

            for (auto& [pod, podSpec] : PodsToUpdate_) {
                PopulateBasicAgentSpec(&podSpec, pod);
                PopulateDynamicPodAttributes(&podSpec, pod);
            }

            FillResponse();

            ReconcileNodeResources();
        }

    private:
        TImpl* const Owner_;
        const TTransactionPtr Transaction_;
        TNode* const Node_;
        const TEpochId EpochId_;
        ui64 SequenceNumber_;
        const TReqHeartbeat* const Request_;
        TRspHeartbeat* const Response_;

        const NLogging::TLogger Logger;

        THashMap<TPod*, NClient::NNodes::NProto::TPodSpec> PodsToUpdate_;
        std::vector<TPod*> PodsToKeep_;
        std::vector<TObjectId> PodIdsToRemove_;
        THashSet<TObjectId> ReportedPodIds_;
        THashSet<TObjectId> UpToDatePodIds_;
        THashMap<TObjectId, TPod*> ExpectedPods_;

        THashSet<TPersistentVolume*> PersistentVolumesToCreate_;
        THashSet<TPersistentVolume*> PersistentVolumesToUpdate_;
        std::vector<TObjectId> PersistentVolumeIdsToRemove_;

        void ScheduleUpdatePod(TPod* pod)
        {
            YT_VERIFY(PodsToUpdate_.emplace(pod, NClient::NNodes::NProto::TPodSpec()).second);
            SchedulePreloadPodSpec(pod);
        }

        void ScheduleKeepPod(TPod* pod)
        {
            PodsToKeep_.push_back(pod);
            SchedulePreloadPodSpec(pod);
        }

        void ScheduleRemovePod(TObjectId podId)
        {
            PodIdsToRemove_.push_back(std::move(podId));
        }

        void SchedulePreloadPodSpec(TPod* pod)
        {
            pod->Spec().IssPayload().ScheduleLoad();
            pod->Spec().PodAgentPayload().ScheduleLoad();
            pod->Spec().Secrets().ScheduleLoad();
            pod->Spec().DynamicAttributes().ScheduleLoad();
            pod->Spec().Etc().ScheduleLoad();
            pod->Status().Agent().Etc().ScheduleLoad();
        }

        void SkipPodUpdate(TPod* pod)
        {
            PodsToUpdate_.erase(pod);
        }

        template <class F>
        void SkipPodUpdateIf(F predicate)
        {
            for (auto it = PodsToUpdate_.begin(); it != PodsToUpdate_.end(); ) {
                auto jt = it++;
                if (predicate(jt->first)) {
                    PodsToUpdate_.erase(jt);
                }
            }
        }

        void SchedulePreloadNode()
        {
            Node_->Resources().ScheduleLoad();
            Node_->Status().EpochId().ScheduleLoad();
            Node_->Status().HeartbeatSequenceNumber().ScheduleLoad();
            Node_->Status().Pods().ScheduleLoad();
            Node_->Resources().ScheduleLoad();

            for (auto* resource : Node_->Resources().Load()) {
                resource->Status().ActualAllocations().ScheduleLoad();
            }

            for (const auto& podEntry : Request_->pods()) {
                const auto& podId = podEntry.pod_id();
                auto* pod = Transaction_->GetPod(podId);
                pod->ScheduleTombstoneCheck();
            }

            for (auto* disk : Node_->Status().AttachedPersistentDisks().Load()) {
                disk->Volumes().ScheduleLoad();
            }
        }

        void ValidateSequencing()
        {
            auto prevSequenceNumber = Node_->Status().HeartbeatSequenceNumber().Load();
            if (SequenceNumber_ <= prevSequenceNumber) {
                THROW_ERROR_EXCEPTION("Wrong heartbeat sequence number: expected >%v, got %v",
                    prevSequenceNumber,
                    SequenceNumber_)
                    << TErrorAttribute("node_id", Node_->GetId())
                    << TErrorAttribute("epoch_id", EpochId_);
            }

            if (Node_->Status().EpochId().Load() != EpochId_) {
                THROW_ERROR_EXCEPTION("Wrong heartbeat epoch: expected %v, got %v",
                    Node_->Status().EpochId().Load(),
                    EpochId_)
                    << TErrorAttribute("node_id", Node_->GetId());
            }
        }

        void UpdateNode()
        {
            Node_->Status().HeartbeatSequenceNumber() = SequenceNumber_;
            Node_->Status().LastSeenTime() = Now();
        }

        bool CheckUnknownPods()
        {
            Node_->Status().Etc()->clear_unknown_pod_ids();
            for (const auto& podEntry : Request_->pods()) {
                const auto& podId = podEntry.pod_id();
                auto* pod = Transaction_->GetPod(podId);
                if (!pod->DoesExist() && !pod->IsTombstone()) {
                    YT_LOG_DEBUG("Unknown pod reported by agent (PodId: %v)",
                        podId);
                    Node_->Status().Etc()->add_unknown_pod_ids(podId);
                }
            }
            return
                Node_->Status().Etc()->unknown_pod_ids_size() == 0 ||
                Node_->Spec().Load().force_remove_unknown_pods();
        }

        bool CheckUnknownPersistentVolumes()
        {
            Node_->Status().Etc()->clear_unknown_persistent_volume_ids();
            for (const auto& [volumeId, volumeEntry] : Request_->persistent_volumes()) {
                auto* volume = Transaction_->GetPersistentVolume(volumeId);
                if (!volume->DoesExist() && !volume->IsTombstone()) {
                    YT_LOG_DEBUG("Unknown persistent volume reported by agent (VolumeId: %v)",
                        volumeId);
                    Node_->Status().Etc()->add_unknown_persistent_volume_ids(volumeId);
                }
            }
            return
                Node_->Status().Etc()->unknown_persistent_volume_ids_size() == 0 ||
                Node_->Spec().Load().force_remove_unknown_persistent_volumes();
        }

        void ProcessReportedPods()
        {
            for (auto* pod : Node_->Status().Pods().Load()) {
                YT_VERIFY(ExpectedPods_.emplace(pod->GetId(), pod).second);
                pod->Spec().UpdateTimestamp().ScheduleLoad();
            }

            // Actually examine pods from the heartbeat.
            for (const auto& podEntry : Request_->pods()) {
                const auto& podId = podEntry.pod_id();

                auto currentState = static_cast<EPodCurrentState>(podEntry.current_state());
                // TODO(babenko): remove after agents are updated
                if (currentState == EPodCurrentState::Unknown) {
                    currentState = static_cast<EPodCurrentState>(podEntry.status().current_state());
                }

                YT_VERIFY(ReportedPodIds_.insert(podId).second);

                auto podIt = ExpectedPods_.find(podId);
                if (podIt == ExpectedPods_.end()) {
                    if (currentState == EPodCurrentState::Stopped) {
                        YT_LOG_DEBUG("Unexpected pod is stopped, ignored (PodId: %v)",
                            podId);
                    } else {
                        YT_LOG_DEBUG("Unexpected pod found, removal scheduled (PodId: %v, CurrentState: %v)",
                            podId,
                            currentState);
                        ScheduleRemovePod(podId);
                    }
                } else {
                    auto* pod = podIt->second;
                    auto agentTimestamp = podEntry.spec_timestamp();
                    auto masterTimestamp = pod->Spec().UpdateTimestamp().Load();
                    // TODO(babenko): remove after agents are updated
                    if (currentState == EPodCurrentState::Unknown) {
                        currentState = pod->Status().Agent().State().Load();
                    }

                    if (agentTimestamp > masterTimestamp) {
                        THROW_ERROR_EXCEPTION("Node %Qv has pod %Qv with spec timestamp %llx while only timestamp %llx is available at master",
                            Node_->GetId(),
                            podId,
                            agentTimestamp,
                            masterTimestamp);
                    }

                    if (agentTimestamp < masterTimestamp) {
                        YT_LOG_DEBUG("Sending pod spec update (PodId: %v, CurrentState: %v, SpecTimestamp: %llx -> %llx)",
                            podId,
                            currentState,
                            agentTimestamp,
                            masterTimestamp);
                        ScheduleUpdatePod(pod);
                    } else {
                        if (currentState == EPodCurrentState::Started) {
                            YT_VERIFY(UpToDatePodIds_.insert(podId).second);
                        }
                        ScheduleKeepPod(pod);
                    }

                    if (podEntry.has_status()) {
                        YT_LOG_DEBUG("Pod status update received (PodId: %v, CurrentState: %v)",
                            podId,
                            currentState);

                        pod->Status().Agent().State() = currentState;

                        // COMPAT(babenko)
                        if (podEntry.status().has_iss_payload()) {
                            pod->Status().Agent().IssPayload() = podEntry.status().iss_payload();
                        }
                        if (podEntry.status().has_pod_agent_payload()) {
                            pod->Status().Agent().PodAgentPayload() = podEntry.status().pod_agent_payload();
                            pod->Status().Agent().PodAgentPayload()->DiscardUnknownFields();
                        }

                        if (podEntry.status().execution_error().code() != NYT::EErrorCode::OK) {
                            *pod->Status().Agent().Etc()->mutable_execution_error() = podEntry.status().execution_error();
                        } else {
                            pod->Status().Agent().Etc()->clear_execution_error();
                        }

                        *pod->Status().Agent().Etc()->mutable_validation_failures() = podEntry.status().validation_failures();
                    }

                    pod->Status().Agent().Etc()->set_last_heartbeat_time(ToProto<ui64>(Now()));

                    pod->Status().AgentSpecTimestamp() = agentTimestamp;

                    pod->Status().Agent().Etc()->set_current_spec_timestamp(podEntry.current_spec_timestamp());
                    *pod->Status().Agent().Etc()->mutable_current_spec_applied() = podEntry.current_spec_applied();
                }
            }
        }

        void SchedulePodInstall()
        {
            for (const auto& [podId, pod] : ExpectedPods_) {
                if (ReportedPodIds_.find(podId) == ReportedPodIds_.end()) {
                    YT_LOG_DEBUG("Requesting pod install (PodId: %v, SpecTimestamp: %llx)",
                        podId,
                        pod->Spec().UpdateTimestamp().Load());
                    ScheduleUpdatePod(pod);
                }
            }
        }

        void ProcessReportedPersistentDisks()
        {
            THashMap<TObjectId, TPersistentDisk*> expectedDisks;
            for (auto* disk : Node_->Status().AttachedPersistentDisks().Load()) {
                YT_VERIFY(expectedDisks.emplace(disk->GetId(), disk).second);
            }

            // Examine disks from the heartbeat.
            THashSet<TObjectId> reportedDiskIds;
            for (const auto& [diskId, diskEntry] : Request_->persistent_disks()) {
                auto* disk = Transaction_->GetPersistentDisk(diskId);
                if (!disk->DoesExist()) {
                    YT_LOG_DEBUG("Unknown persistent disk is reported by node, ignored (DiskId: %v)",
                        diskId);
                    continue;
                }

                YT_VERIFY(reportedDiskIds.insert(diskId).second);

                if (!expectedDisks.contains(diskId)) {
                    YT_LOG_DEBUG("Persistent disk is attached to node (DiskId: %v, OldAttachedNodeId: %v)",
                        diskId,
                        GetObjectId(disk->Status().AttachedToNode().Load()));
                    Node_->Status().AttachedPersistentDisks().Add(disk);
                }

                auto* statusEtc = disk->Status().Etc().Get();
                statusEtc->set_last_seen_node_id(Node_->GetId());
                *statusEtc->mutable_last_seen_time() = google::protobuf::util::TimeUtil::GetCurrentTime();
            }

            // Check for disks that are expected to be present but are in fact missing.
            for (const auto& [diskId, disk]: expectedDisks) {
                if (!reportedDiskIds.contains(diskId)) {
                    YT_LOG_DEBUG("Persistent disk is detached from node (DiskId: %v)",
                        diskId);
                    Node_->Status().AttachedPersistentDisks().Remove(disk);
                }
            }
        }

        void ScheduleCreatePersistentVolume(TPersistentVolume* volume)
        {
            YT_VERIFY(PersistentVolumesToCreate_.insert(volume).second);
            SchedulePreloadPersistentVolumeSpec(volume);
        }

        void ScheduleUpdatePersistentVolume(TPersistentVolume* volume)
        {
            YT_VERIFY(PersistentVolumesToCreate_.insert(volume).second);
            SchedulePreloadPersistentVolumeSpec(volume);
        }

        void SchedulePreloadPersistentVolumeSpec(TPersistentVolume* volume)
        {
            volume->Spec().Etc().ScheduleLoad();
        }

        void ScheduleRemovePersistentVolume(TObjectId volumeId)
        {
            PersistentVolumeIdsToRemove_.push_back(std::move(volumeId));
        }

        void ProcessReportedPersistentVolumes()
        {
            THashMap<TObjectId, TPersistentVolume*> expectedVolumes;
            for (auto* disk : Node_->Status().AttachedPersistentDisks().Load()) {
                for (auto* volume : disk->Volumes().Load()) {
                    YT_VERIFY(expectedVolumes.emplace(volume->GetId(), volume).second);
                }
            }

            // Examine volumes from the heartbeat.
            THashSet<TObjectId> reportedVolumeIds;
            for (const auto& [volumeId, volumeEntry] : Request_->persistent_volumes()) {
                YT_VERIFY(reportedVolumeIds.insert(volumeId).second);
                const auto& diskId = volumeEntry.disk_id();
                auto it = expectedVolumes.find(volumeId);
                if (it == expectedVolumes.end()) {
                    YT_LOG_DEBUG("Unknown persistent volume is reported by node, scheduling removal (DiskId: %v, VolumeId: %v)",
                        diskId,
                        volumeId);
                    ScheduleRemovePersistentVolume(volumeId);
                    continue;
                }

                auto* volume = it->second;
                const auto& expectedDiskId = volume->Disk().Load()->GetId();
                if (diskId != expectedDiskId) {
                    THROW_ERROR_EXCEPTION("Node reported persistent volume %Qv present at disk %Qv while disk %Qv was expected",
                        volumeId,
                        diskId,
                        expectedDiskId);
                }

                auto agentTimestamp = volumeEntry.current_spec_timestamp();
                auto masterTimestamp = volume->Spec().LoadTimestamp();

                auto* etc = volume->Status().Etc().Get();
                etc->set_agent_spec_timestamp(volumeEntry.current_spec_timestamp());

                if (agentTimestamp > masterTimestamp) {
                    THROW_ERROR_EXCEPTION("Node %Qv has persistent volume %Qv with spec timestamp %llx while only timestamp %llx is available at master",
                        Node_->GetId(),
                        volumeId,
                        agentTimestamp,
                        masterTimestamp);
                }

                if (volumeEntry.has_status()) {
                    etc->set_state(volumeEntry.status().state());
                    if (volumeEntry.status().has_execution_error()) {
                        etc->mutable_execution_error()->CopyFrom(volumeEntry.status().execution_error());
                    } else {
                        etc->clear_execution_error();
                    }
                    etc->mutable_validation_failures()->CopyFrom(volumeEntry.status().validation_failures());
                }

                if (agentTimestamp == masterTimestamp) {
                    continue;
                }

                YT_LOG_DEBUG("Sending persistent volume spec update (VolumeId: %v, SpecTimestamp: %llx -> %llx)",
                    volumeId,
                    agentTimestamp,
                    masterTimestamp);
                ScheduleUpdatePersistentVolume(volume);
            }

            // Check for volumes that are expected to be present but are in fact missing.
            for (const auto& [volumeId, volume]: expectedVolumes) {
                if (reportedVolumeIds.contains(volumeId)) {
                    continue;
                }

                auto* disk = volume->Disk().Load();
                YT_LOG_DEBUG("Persistent volume is missing at node, creation scheduled (DiskId: %v, VolumeId: %v)",
                    disk->GetId(),
                    volumeId);
                ScheduleCreatePersistentVolume(volume);
            }
        }

        void AnalyzePodInstallErrors()
        {
            SkipPodUpdateIf([&] (auto* pod) {
                const auto& etc = pod->Status().Agent().Etc().Load();
                if (etc.has_install_error() &&
                    etc.failed_install_attempt_spec_timestamp() == pod->Spec().UpdateTimestamp().Load())
                {
                    YT_LOG_DEBUG("Pod update skipped due to an active install error (PodId: %v)",
                        pod->GetId());
                    return true;
                } else {
                    ResetPodInstallFailure(pod);
                    return false;
                }
            });
        }

        void SetPodInstallFailure(TPod* pod, const TError& error)
        {
            if (PodsToUpdate_.erase(pod) == 0) {
                return;
            }
            YT_LOG_DEBUG(error, "Pod install failed (PodId: %v)",
                pod->GetId());
            auto* etc = pod->Status().Agent().Etc().Get();
            ToProto(etc->mutable_install_error(), error);
            etc->set_failed_install_attempt_spec_timestamp(pod->Spec().UpdateTimestamp().Load());
        }

        void ResetPodInstallFailure(TPod* pod)
        {
            auto* etc = pod->Status().Agent().Etc().Get();
            etc->clear_install_error();
            etc->clear_failed_install_attempt_spec_timestamp();
        }

        static bool IsPersistentSecretVaultError(const TError& error)
        {
            return
                error.FindMatching(ESecretVaultErrorCode::NonexistentEntityError) ||
                error.FindMatching(ESecretVaultErrorCode::DelegationAccessError) ||
                error.FindMatching(ESecretVaultErrorCode::DelegationTokenRevoked);
        }

        void RequestSecrets()
        {
            // These vectors are aligned as follows:
            // secretSubrequests[i] contains a secret request issued by podsRequestingSecrets[i]
            // and keyed as secretKeys[i].
            std::vector<ISecretVaultService::TSecretSubrequest> secretSubrequests;
            std::vector<TString> secretKeys;
            std::vector<TPod*> podsRequestingSecrets;
            for (const auto& [pod, podSpec] : PodsToUpdate_) {
                const auto& secrets = pod->Spec().Secrets().Load();
                for (const auto& [secretKey, secret] : secrets) {
                    secretKeys.push_back(secretKey);
                    secretSubrequests.push_back({
                        secret.secret_id(),
                        secret.secret_version(),
                        secret.delegation_token(),
                        pod->PodSet().Load()->GetId()
                    });
                    podsRequestingSecrets.push_back(pod);
                }
            }

            if (!secretSubrequests.empty()) {
                YT_LOG_DEBUG("Retrieving secrets from Vault (Count: %v)",
                    secretSubrequests.size());
                const auto& secretVaultService = Owner_->Bootstrap_->GetSecretVaultService();
                auto result = WaitFor(secretVaultService->GetSecrets(secretSubrequests));
                if (result.IsOK()) {
                    const auto& secretSubresponsesOrErrors = result.Value();
                    for (size_t index = 0; index < secretSubresponsesOrErrors.size(); ++index) {
                        auto* pod = podsRequestingSecrets[index];

                        auto podIt = PodsToUpdate_.find(pod);
                        if (podIt == PodsToUpdate_.end()) {
                            continue;
                        }
                        auto& podSpec = podIt->second;

                        const auto& secretSubresponseOrError = secretSubresponsesOrErrors[index];
                        if (secretSubresponseOrError.IsOK()) {
                            auto& secretSubresponse = secretSubresponseOrError.Value();
                            auto* protoSecret = podSpec.add_secrets();
                            protoSecret->set_id(secretKeys[index]);
                            for (auto& [key, value] : secretSubresponse.Payload) {
                                (*protoSecret->mutable_payload())[key] = std::move(value);
                            }
                        } else if (IsPersistentSecretVaultError(secretSubresponseOrError)) {
                            auto error = TError("Error retrieving secrets from Vault")
                                << secretSubresponseOrError;
                            SetPodInstallFailure(pod, error);
                        } else {
                            YT_LOG_DEBUG(secretSubresponseOrError, "Pod update skipped due to transient Vault error (PodId: %v)",
                                pod->GetId());
                            SkipPodUpdate(pod);
                        }
                    }
                } else {
                    YT_LOG_WARNING(result, "Vault request failed");
                    SkipPodUpdateIf([&] (TPod* pod) {
                        if (pod->Spec().Secrets().Load().empty()) {
                            return false;
                        }
                        YT_LOG_DEBUG("Pod update skipped due to Vault error (PodId: %v)",
                            pod->GetId());
                        return true;
                    });
                }
            }
        }

        void PopulateBasicAgentSpec(
            NClient::NNodes::NProto::TPodSpec* protoSpec,
            TPod* pod)
        {
            auto* cpuResource = Node_->GetCpuResourceOrThrow();
            auto properties = BuildPortoProperties(
                cpuResource->Spec().Load().cpu(),
                pod->Spec().Etc().Load(),
                pod->Status().Etc().Load());
            for (const auto& [key, value] : properties) {
                auto* protoProperty = protoSpec->add_porto_properties();
                protoProperty->set_key(key);
                protoProperty->set_value(value);
                YT_LOG_DEBUG("Setting Porto property (PodId: %v, Name: %v, Value: %v)",
                    pod->GetId(),
                    key,
                    value);
            };

            // Payload
            // COMPAT(babenko)
            if (pod->Spec().IssPayload().Load()) {
                protoSpec->set_iss_payload(pod->Spec().IssPayload().Load());
            } else {
                *protoSpec->mutable_pod_agent_payload() = pod->Spec().PodAgentPayload().Load();
            }

            // ResourceCache
            *protoSpec->mutable_resource_cache() = pod->Spec().ResourceCache().Load();

            // Copy some fields from pod status/spec.
            const auto& specEtc = pod->Spec().Etc().Load();
            const auto& statusEtc = pod->Status().Etc().Load();
            protoSpec->mutable_resource_requests()->CopyFrom(specEtc.resource_requests());
            protoSpec->mutable_ip6_address_requests()->CopyFrom(specEtc.ip6_address_requests());
            protoSpec->mutable_ip6_subnet_requests()->CopyFrom(specEtc.ip6_subnet_requests());
            protoSpec->mutable_ip6_address_allocations()->CopyFrom(statusEtc.ip6_address_allocations());
            protoSpec->mutable_ip6_subnet_allocations()->CopyFrom(statusEtc.ip6_subnet_allocations());
            protoSpec->mutable_dns()->CopyFrom(statusEtc.dns());
            protoSpec->mutable_disk_volume_allocations()->CopyFrom(statusEtc.disk_volume_allocations());
            protoSpec->mutable_disk_volume_mounts()->CopyFrom(statusEtc.disk_volume_mounts());
            protoSpec->mutable_gpu_allocations()->CopyFrom(statusEtc.gpu_allocations());
            protoSpec->mutable_host_infra()->CopyFrom(specEtc.host_infra());

            if (specEtc.resource_requests().has_network_bandwidth_guarantee()) {
                protoSpec->mutable_network_allocation()->set_tx_bandwidth_guarantee(
                    specEtc.resource_requests().network_bandwidth_guarantee());
            }
            if (specEtc.resource_requests().has_network_bandwidth_limit()) {
                protoSpec->mutable_network_allocation()->set_tx_bandwidth_limit(
                    specEtc.resource_requests().network_bandwidth_limit());
                protoSpec->mutable_network_allocation()->set_rx_bandwidth_limit(
                    specEtc.resource_requests().network_bandwidth_limit());
            }

            for (const auto& capability : specEtc.capabilities()) {
                *protoSpec->add_capabilities() = capability;
            }
        }

        void PopulateDynamicPodAttributes(
            NClient::NNodes::NProto::TPodSpec* agentSpec,
            TPod* pod)
        {
            *agentSpec->mutable_pod_dynamic_attributes() = BuildPodDynamicAttributes(pod);
        }

        void PopulatePersistentVolumeSpec(
            TPersistentVolume* volume,
            NClient::NNodes::NProto::TPersistentVolumeSpec* agentSpec)
        {
            auto* disk = volume->Disk().Load();
            const auto& volumeSpecEtc = volume->Spec().Etc().Load();
            if (volumeSpecEtc.has_managed_policy()) {
                const auto& masterVolumePolicy = volumeSpecEtc.managed_policy();
                if (!disk->Spec().Etc().Load().has_managed_policy()) {
                    THROW_ERROR_EXCEPTION("Persistent %Qv disk and its volume %Qv have mismatching kinds",
                        disk->GetId(),
                        volume->GetId());
                }
                const auto& masterDiskPolicy = disk->Spec().Etc().Load().managed_policy();
                auto* agentPolicy = agentSpec->mutable_managed_policy();

                agentPolicy->set_capacity(masterVolumePolicy.capacity());

                agentPolicy->set_read_bandwidth_guarantee(masterVolumePolicy.bandwidth_guarantee() * masterDiskPolicy.read_bandwidth_factor());
                agentPolicy->set_write_bandwidth_guarantee(masterVolumePolicy.bandwidth_guarantee() * masterDiskPolicy.write_bandwidth_factor());
                agentPolicy->set_read_bandwidth_limit(masterVolumePolicy.bandwidth_limit() * masterDiskPolicy.read_bandwidth_factor());
                agentPolicy->set_write_bandwidth_limit(masterVolumePolicy.bandwidth_limit() * masterDiskPolicy.write_bandwidth_factor());

                auto getReadOperationRateFactor = [&] {
                    return masterDiskPolicy.read_operation_rate_divisor() != 0.0
                        ? 1.0 / masterDiskPolicy.read_operation_rate_divisor()
                        : 0.0;
                };
                auto getWriteOperationRateFactor = [&] {
                    return masterDiskPolicy.write_operation_rate_divisor() != 0.0
                        ? 1.0 / masterDiskPolicy.write_operation_rate_divisor()
                        : 0.0;
                };
                agentPolicy->set_read_operation_rate_guarantee(masterVolumePolicy.operation_rate_guarantee() * getReadOperationRateFactor());
                agentPolicy->set_write_operation_rate_guarantee(masterVolumePolicy.operation_rate_guarantee() * getWriteOperationRateFactor());
                agentPolicy->set_read_operation_rate_limit(masterVolumePolicy.operation_rate_limit() * getReadOperationRateFactor());
                agentPolicy->set_write_operation_rate_limit(masterVolumePolicy.operation_rate_limit() * getWriteOperationRateFactor());
            } else if (volumeSpecEtc.has_rbind_policy()) {
                const auto& masterPolicy = volumeSpecEtc.rbind_policy();
                auto* agentPolicy = agentSpec->mutable_rbind_policy();
                agentPolicy->set_mount_path(masterPolicy.mount_path());
            } else {
                THROW_ERROR_EXCEPTION("Persistent volume %Qv has spec of unknown kind",
                    volume->GetId());
            }
        }

        void FillResponse()
        {
            for (auto& [pod, podSpec] : PodsToUpdate_) {
                auto* podEntry = Response_->add_pods();
                ToProto(podEntry->mutable_pod_id(), pod->GetId());
                podEntry->set_spec_timestamp(pod->Spec().UpdateTimestamp().Load());
                podEntry->mutable_spec()->set_target_state(NClient::NApi::NProto::PTS_ACTIVE);
                podEntry->set_target_state(NClient::NApi::NProto::PTS_ACTIVE);
                podEntry->mutable_spec()->Swap(&podSpec);
            }

            for (auto* pod : PodsToKeep_) {
                auto* podEntry = Response_->add_pods();
                ToProto(podEntry->mutable_pod_id(), pod->GetId());
                podEntry->set_spec_timestamp(pod->Spec().UpdateTimestamp().Load());
                podEntry->set_target_state(NClient::NApi::NProto::PTS_ACTIVE);
            }

            for (const auto& podId : PodIdsToRemove_) {
                auto* podEntry = Response_->add_pods();
                ToProto(podEntry->mutable_pod_id(), podId);
                podEntry->mutable_spec()->set_target_state(NClient::NApi::NProto::PTS_REMOVED);
                podEntry->set_target_state(NClient::NApi::NProto::PTS_REMOVED);
            }

            for (auto* volume : PersistentVolumesToCreate_) {
                auto& request = (*Response_->mutable_persistent_volume_creation_requests())[volume->GetId()];
                request.set_disk_id(volume->Disk().Load()->GetId());
                request.set_spec_timestamp(volume->Spec().LoadTimestamp());
                PopulatePersistentVolumeSpec(volume, request.mutable_spec());
            }

            for (auto* volume : PersistentVolumesToUpdate_) {
                auto& request = (*Response_->mutable_persistent_volume_update_requests())[volume->GetId()];
                request.set_spec_timestamp(volume->Spec().LoadTimestamp());
                PopulatePersistentVolumeSpec(volume, request.mutable_spec());
            }

            for (const auto& volumeId : PersistentVolumeIdsToRemove_) {
                (*Response_->mutable_persistent_volume_removal_requests())[volumeId];
            }

            Response_->mutable_node()->mutable_cpu()->CopyFrom(Node_->GetCpuResourceOrThrow()->Spec().Load().cpu());
        }

        void ReconcileNodeResources()
        {
            for (auto* resource : Node_->Resources().Load()) {
                const auto& scheduledAllocations = resource->Status().ScheduledAllocations().Load();
                const auto& oldActualAllocations = resource->Status().ActualAllocations().Load();

                auto newActualAllocations = oldActualAllocations;

                // Drop actual allocations for unknown pods and also for up-to-date pods (these allocations
                // will be copied from scheduled ones).
                newActualAllocations.erase(
                    std::remove_if(
                        newActualAllocations.begin(),
                        newActualAllocations.end(),
                        [&] (const auto& allocation) {
                            const auto& podId = allocation.pod_id();
                            return
                                ReportedPodIds_.find(podId) == ReportedPodIds_.end() ||
                                UpToDatePodIds_.find(podId) != UpToDatePodIds_.end();
                        }),
                    newActualAllocations.end());

                // Copy scheduled allocations for the up-to-date pods to the actual ones.
                for (const auto& scheduledAllocation : scheduledAllocations) {
                    const auto& podId = scheduledAllocation.pod_id();
                    if (UpToDatePodIds_.find(podId) == UpToDatePodIds_.end()) {
                        continue;
                    }
                    newActualAllocations.emplace_back().MergeFrom(scheduledAllocation);
                }

                if (newActualAllocations != oldActualAllocations) {
                    resource->Status().ActualAllocations() = std::move(newActualAllocations);
                }
            }
        }
    };

    void ProcessHeartbeat(
        const TTransactionPtr& transaction,
        TNode* node,
        const TEpochId& epochId,
        ui64 sequenceNumber,
        const TReqHeartbeat* request,
        TRspHeartbeat* response)
    {
        THeartbeatHandler handler(
            this,
            transaction,
            node,
            epochId,
            sequenceNumber,
            request,
            response);
        handler.Run();
    }

    void NotifyAgent(TNode* node)
    {
        if (!Config_->EnableAgentNotification) {
            return;
        }

        TAgentNotificationQueueEntry entry{
            node->GetId(),
            node->Status().AgentAddress().Load()
        };

        YT_LOG_DEBUG("Agent notification enqueued (NodeId: %v, Address: %v)",
            entry.Id,
            entry.Address);

        {
            auto guard = Guard(AgentNotificationQueueLock_);
            AgentNotificationQueue_.emplace(std::move(entry));
        }
    }

private:
    TBootstrap* const Bootstrap_;
    const TNodeTrackerConfigPtr Config_;

    const IChannelFactoryPtr NodeChannelFactory_ = CreateCachingChannelFactory(GetGrpcChannelFactory());

    const NConcurrency::TPeriodicExecutorPtr AgentNotificationExecutor_;
    const NConcurrency::IThroughputThrottlerPtr AgentNotificationThrottler_;

    struct TAgentNotificationQueueEntry
    {
        TObjectId Id;
        TString Address;
    };

    TSpinLock  AgentNotificationQueueLock_;
    std::queue<TAgentNotificationQueueEntry> AgentNotificationQueue_;

    const NLogging::TLogger& Logger = NNodes::Logger;


    std::optional<TAgentNotificationQueueEntry> PeekAgentNotificationQueue()
    {
        auto guard = Guard(AgentNotificationQueueLock_);
        if (AgentNotificationQueue_.empty()) {
            return std::nullopt;
        }
        if (!AgentNotificationThrottler_->TryAcquire(1)) {
            return std::nullopt;
        }
        auto entry = std::move(AgentNotificationQueue_.front());
        AgentNotificationQueue_.pop();
        return entry;
    }

    void OnAgentNotificationTick()
    {
        while (auto optionalEntry = PeekAgentNotificationQueue()) {
            const auto& entry = *optionalEntry;

            YT_LOG_DEBUG("Sending agent notification (NodeId: %v, Address: %v)",
                entry.Id,
                entry.Address);

            auto proxy = CreateAgentProxy(entry.Address);
            auto req = proxy->Notify();
            return req->Invoke().Subscribe(BIND([=, this_ = MakeStrong(this)] (const TAgentServiceProxy::TErrorOrRspNotifyPtr& rspOrError) {
                if (rspOrError.IsOK()) {
                    YT_LOG_DEBUG("Agent notification succeeded (NodeId: %v, Address: %v)",
                        entry.Id,
                        entry.Address);
                } else {
                    YT_LOG_DEBUG(rspOrError, "Agent notification failed (NodeId: %v, Address: %v)",
                        entry.Id,
                        entry.Address);
                }
            }));
        }
    }

    std::unique_ptr<TAgentServiceProxy> CreateAgentProxy(const TString& address)
    {
        auto channel = NodeChannelFactory_->CreateChannel(address);
        auto proxy = std::make_unique<TAgentServiceProxy>(std::move(channel));
        proxy->SetDefaultTimeout(Config_->AgentNotificationRpcTimeout);
        return proxy;
    }
};

////////////////////////////////////////////////////////////////////////////////

TNodeTracker::TNodeTracker(TBootstrap* bootstrap, TNodeTrackerConfigPtr config)
    : Impl_(New<TImpl>(bootstrap, std::move(config)))
{ }

TNodeTracker::~TNodeTracker()
{ }

TNode* TNodeTracker::ProcessHandshake(
    const TTransactionPtr& transaction,
    const TObjectId& nodeId,
    const TString& address,
    const TString& version)
{
    return Impl_->ProcessHandshake(
        transaction,
        nodeId,
        address,
        version);
}

void TNodeTracker::ProcessHeartbeat(
    const TTransactionPtr& transaction,
    TNode* node,
    const TEpochId& epochId,
    ui64 sequenceNumber,
    const TReqHeartbeat* request,
    TRspHeartbeat* response)
{
    Impl_->ProcessHeartbeat(
        transaction,
        node,
        epochId,
        sequenceNumber,
        request,
        response);
}

void TNodeTracker::NotifyAgent(TNode* Node_)
{
    Impl_->NotifyAgent(Node_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NNodes

