#include "node_tracker.h"
#include "porto.h"
#include "private.h"

#include <yp/server/objects/transaction.h>
#include <yp/server/objects/node.h>
#include <yp/server/objects/pod.h>
#include <yp/server/objects/pod_set.h>
#include <yp/server/objects/resource.h>

#include <yp/server/master/config.h>
#include <yp/server/master/bootstrap.h>

#include <yp/client/nodes/node_tracker_service_proxy.h>
#include <yp/client/nodes/agent_service_proxy.h>

#include <yt/ytlib/auth/secret_vault_service.h>

#include <yt/core/rpc/caching_channel_factory.h>

#include <yt/core/rpc/grpc/channel.h>

#include <yt/core/concurrency/throughput_throttler.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <queue>

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

        Node_->Status().AgentAddress() = address;
        Node_->Status().LastSeenTime() = Now();
        Node_->Status().EpochId() = TEpochId::Create();
        Node_->Status().HeartbeatSequenceNumber() = 0;
        Node_->Status().Other()->set_agent_version(version);

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
            TNode* Node_,
            TEpochId epochId,
            ui64 sequenceNumber,
            const TReqHeartbeat* request,
            TRspHeartbeat* response)
            : Owner_(owner)
            , Transaction_(std::move(transaction))
            , Node_(Node_)
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
            SchedulePreload();

            ValidateSequencing();

            YT_LOG_DEBUG("Heartbeat received (EpochId: %v, SequenceNumber: %v)",
                EpochId_,
                SequenceNumber_);

            UpdateNode();

            CheckUnknownPods();

            if (Node_->Status().Other()->unknown_pod_ids_size() > 0 &&
                !Node_->Spec().Load().force_remove_unknown_pods())
            {
                return;
            }

            ProcessReportedPods();

            SchedulePodInstall();

            AnalyzePodInstallErrors();

            RequestSecrets();

            for (auto& pair : PodsToUpdate_) {
                auto* pod = pair.first;
                PopulateBasicAgentSpec(&pair.second, pod);
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


        void ScheduleUpdatePod(TPod* pod)
        {
            YCHECK(PodsToUpdate_.emplace(pod, NClient::NNodes::NProto::TPodSpec()).second);
            PreprarePodUpdate(pod);
        }

        void ScheduleKeepPod(TPod* pod)
        {
            PodsToKeep_.push_back(pod);
        }

        void ScheduleRemovePod(TObjectId podId)
        {
            PodIdsToRemove_.push_back(std::move(podId));
        }

        void PreprarePodUpdate(TPod* pod)
        {
            pod->Spec().IssPayload().ScheduleLoad();
            pod->Spec().PodAgentPayload().ScheduleLoad();
            pod->Spec().Secrets().ScheduleLoad();
            pod->Spec().Other().ScheduleLoad();
            pod->Status().Agent().Other().ScheduleLoad();
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

        void SchedulePreload()
        {
            Node_->Resources().ScheduleLoad();
            Node_->Status().EpochId().ScheduleLoad();
            Node_->Status().HeartbeatSequenceNumber().ScheduleLoad();
            Node_->Pods().ScheduleLoad();
            Node_->Resources().ScheduleLoad();

            for (auto* resource : Node_->Resources().Load()) {
                resource->Status().ActualAllocations().ScheduleLoad();
            }

            for (const auto& podEntry : Request_->pods()) {
                auto podId = FromProto<TObjectId>(podEntry.pod_id());
                auto* pod = Transaction_->GetPod(podId);
                pod->ScheduleTombstoneCheck();
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

        void CheckUnknownPods()
        {
            Node_->Status().Other()->clear_unknown_pod_ids();
            for (const auto& podEntry : Request_->pods()) {
                auto podId = FromProto<TObjectId>(podEntry.pod_id());
                auto* pod = Transaction_->GetPod(podId);
                if (!pod->DoesExist() && !pod->IsTombstone()) {
                    YT_LOG_DEBUG("Unknown pod reported by agent (PodId: %v)",
                        podId);
                    Node_->Status().Other()->add_unknown_pod_ids(podId);
                }
            }
        }

        void ProcessReportedPods()
        {
            for (auto* pod : Node_->Pods().Load()) {
                YCHECK(ExpectedPods_.emplace(pod->GetId(), pod).second);
                pod->Spec().UpdateTimestamp().ScheduleLoad();
            }

            // Actually examine pods from the heartbeat.
            for (const auto& podEntry : Request_->pods()) {
                auto podId = FromProto<TObjectId>(podEntry.pod_id());

                auto currentState = static_cast<EPodCurrentState>(podEntry.current_state());
                // TODO(babenko): remove after agents are updated
                if (currentState == EPodCurrentState::Unknown) {
                    currentState = static_cast<EPodCurrentState>(podEntry.status().current_state());
                }

                YCHECK(ReportedPodIds_.insert(podId).second);

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
                            YCHECK(UpToDatePodIds_.insert(podId).second);
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
                        }

                        if (podEntry.status().execution_error().code() != NYT::EErrorCode::OK) {
                            *pod->Status().Agent().Other()->mutable_execution_error() = podEntry.status().execution_error();
                        } else {
                            pod->Status().Agent().Other()->clear_execution_error();
                        }

                        *pod->Status().Agent().Other()->mutable_validation_failures() = podEntry.status().validation_failures();
                    }

                    pod->Status().Agent().Other()->set_last_heartbeat_time(ToProto<ui64>(Now()));

                    pod->Status().AgentSpecTimestamp() = agentTimestamp;
                }
            }
        }

        void SchedulePodInstall()
        {
            for (const auto& pair : ExpectedPods_) {
                const auto& podId = pair.first;
                auto* pod = pair.second;
                if (ReportedPodIds_.find(podId) == ReportedPodIds_.end()) {
                    YT_LOG_DEBUG("Requesting pod install (PodId: %v, SpecTimestamp: %llx)",
                        podId,
                        pod->Spec().UpdateTimestamp().Load());
                    ScheduleUpdatePod(pod);
                }
            }
        }

        void AnalyzePodInstallErrors()
        {
            SkipPodUpdateIf([&] (auto* pod) {
                const auto& other = pod->Status().Agent().Other().Load();
                if (other.has_install_error() &&
                    other.failed_install_attempt_spec_timestamp() == pod->Spec().UpdateTimestamp().Load())
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
            auto* other = pod->Status().Agent().Other().Get();
            ToProto(other->mutable_install_error(), error);
            other->set_failed_install_attempt_spec_timestamp(pod->Spec().UpdateTimestamp().Load());
        }

        void ResetPodInstallFailure(TPod* pod)
        {
            auto* other = pod->Status().Agent().Other().Get();
            other->clear_install_error();
            other->clear_failed_install_attempt_spec_timestamp();
        }

        static bool IsPersistentSecretVaultError(const TError& error)
        {
            return
                error.FindMatching(ESecretVaultErrorCode::DelegationAccessError) ||
                error.FindMatching(ESecretVaultErrorCode::DelegationTokenRevoked);
        }

        void RequestSecrets()
        {
            std::vector<ISecretVaultService::TSecretSubrequest> secretSubrequests;
            std::vector<TPod*> podsRequestingSecrets;
            for (const auto& pair : PodsToUpdate_) {
                auto* pod = pair.first;
                const auto& secrets = pod->Spec().Secrets().Load();
                for (const auto& pair : secrets) {
                    const auto& secret = pair.second;
                    secretSubrequests.push_back({
                        secret.secret_id(),
                        secret.secret_version(),
                        secret.delegation_token(),
                        pod->PodSet().Load()->GetId()
                    });
                    podsRequestingSecrets.push_back(pod);
                }
            }

            std::vector<TKeyedSecret> keyedSecrets(secretSubrequests.size());
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
                            const auto& keyedSecret = keyedSecrets[index];
                            auto* protoSecret = podSpec.add_secrets();
                            protoSecret->set_id(keyedSecret.first);
                            for (auto& pair : secretSubresponse.Payload) {
                                (*protoSecret->mutable_payload())[pair.first] = std::move(pair.second);
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
                pod->Spec().Other().Load(),
                pod->Status().Other().Load());
            for (const auto& pair : properties) {
                auto* protoProperty = protoSpec->add_porto_properties();
                protoProperty->set_key(pair.first);
                protoProperty->set_value(pair.second);
                YT_LOG_DEBUG("Setting Porto property (PodId: %v, Name: %v, Value: %v)",
                    pod->GetId(),
                    pair.first,
                    pair.second);
            };

            // Payload
            // COMPAT(babenko)
            if (pod->Spec().IssPayload().Load()) {
                protoSpec->set_iss_payload(pod->Spec().IssPayload().Load());
            } else {
                *protoSpec->mutable_pod_agent_payload() = pod->Spec().PodAgentPayload().Load();
            }

            // Copy some fields from pod status/spec.
            const auto& specOther = pod->Spec().Other().Load();
            const auto& statusOther = pod->Status().Other().Load();
            protoSpec->mutable_ip6_address_requests()->CopyFrom(specOther.ip6_address_requests());
            protoSpec->mutable_ip6_subnet_requests()->CopyFrom(specOther.ip6_subnet_requests());
            protoSpec->mutable_ip6_address_allocations()->CopyFrom(statusOther.ip6_address_allocations());
            protoSpec->mutable_ip6_subnet_allocations()->CopyFrom(statusOther.ip6_subnet_allocations());
            protoSpec->mutable_dns()->CopyFrom(statusOther.dns());
            protoSpec->mutable_disk_volume_allocations()->CopyFrom(statusOther.disk_volume_allocations());
        }

        void FillResponse()
        {
            for (auto& pair : PodsToUpdate_) {
                auto* pod = pair.first;
                auto* podEntry = Response_->add_pods();
                ToProto(podEntry->mutable_pod_id(), pod->GetId());
                podEntry->set_spec_timestamp(pod->Spec().UpdateTimestamp().Load());
                podEntry->mutable_spec()->set_target_state(NClient::NApi::NProto::PTS_ACTIVE);
                podEntry->set_target_state(NClient::NApi::NProto::PTS_ACTIVE);
                podEntry->mutable_spec()->Swap(&pair.second);
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
        }

        void ReconcileNodeResources()
        {
            for (auto* resource : Node_->Resources().Load()) {
                const auto& scheduledAllocations = resource->Status().ScheduledAllocations().Load();
                auto* actualAllocations = resource->Status().ActualAllocations().Get();

                // Drop actual allocations for unknown pods and also for up-to-date pods (these allocations
                // will be copied from scheduled ones).
                actualAllocations->erase(
                    std::remove_if(
                        actualAllocations->begin(),
                        actualAllocations->end(),
                        [&] (const auto& allocation) {
                            auto podId = FromProto<TObjectId>(allocation.pod_id());
                            return
                                ReportedPodIds_.find(podId) == ReportedPodIds_.end() ||
                                UpToDatePodIds_.find(podId) != UpToDatePodIds_.end();
                        }),
                    actualAllocations->end());

                // Copy scheduled allocations for the up-to-date pods to the actual ones.
                for (const auto& scheduledAllocation : scheduledAllocations) {
                    auto podId = FromProto<TObjectId>(scheduledAllocation.pod_id());
                    if (UpToDatePodIds_.find(podId) == UpToDatePodIds_.end()) {
                        continue;
                    }
                    actualAllocations->emplace_back();
                    actualAllocations->back().MergeFrom(scheduledAllocation);
                }
            }
        }
    };

    using TKeyedSecret = std::pair<TString, ISecretVaultService::TSecretSubrequest>;

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

        AgentNotificationQueue_.emplace(std::move(entry));
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
    std::queue<TAgentNotificationQueueEntry> AgentNotificationQueue_;

    const NLogging::TLogger& Logger = NNodes::Logger;


    void OnAgentNotificationTick()
    {
        while (!AgentNotificationQueue_.empty() && AgentNotificationThrottler_->TryAcquire(1)) {
            auto entry = std::move(AgentNotificationQueue_.front());
            AgentNotificationQueue_.pop();

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
    TNode* Node_,
    const TEpochId& epochId,
    ui64 sequenceNumber,
    const TReqHeartbeat* request,
    TRspHeartbeat* response)
{
    Impl_->ProcessHeartbeat(
        transaction,
        Node_,
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

