#include "node_tracker.h"
#include "porto.h"
#include "private.h"

#include <yp/server/objects/transaction.h>
#include <yp/server/objects/node.h>
#include <yp/server/objects/pod.h>
#include <yp/server/objects/resource.h>

#include <yp/server/master/config.h>
#include <yp/server/master/bootstrap.h>

#include <yp/client/nodes/node_tracker_service_proxy.h>
#include <yp/client/nodes/agent_service_proxy.h>

#include <yt/core/rpc/caching_channel_factory.h>

#include <yt/core/rpc/grpc/channel.h>

#include <yt/core/concurrency/throughput_throttler.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <queue>

namespace NYP {
namespace NServer {
namespace NNodes {

using namespace NServer::NObjects;
using namespace NServer::NMaster;
using namespace NClient::NNodes;
using namespace NClient::NNodes::NProto;
using namespace NClient::NApi::NProto;
using namespace NYT::NRpc;
using namespace NYT::NRpc::NGrpc;
using namespace NYT::NConcurrency;

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
        : Config_(std::move(config))
        , AgentNotificationExecutor_(New<TPeriodicExecutor>(
            bootstrap->GetControlInvoker(),
            BIND(&TImpl::OnAgentNotificationTick, MakeWeak(this)),
            AgentNotificationTick))
        , AgentNotificationThrottler_(CreateReconfigurableThroughputThrottler(
            Config_->AgentNotificationThrottler,
            {},
            NProfiling::TProfiler(Profiler.GetPathPrefix() + "/agent_notification_throttler")))
    {
        AgentNotificationExecutor_->Start();
    }

    TNode* ProcessHandshake(
        const TTransactionPtr& transaction,
        const TString& nodeId,
        const TString& address)
    {
        auto* node = transaction->GetNode(nodeId);
        node->ValidateExists();

        node->Status().AgentAddress() = address;
        node->Status().LastSeenTime() = TInstant::Now();
        node->Status().EpochId() = TEpochId::Create();
        node->Status().HeartbeatSequenceNumber() = 0;

        LOG_DEBUG("Handshake received (NodeId: %v, Address: %v, EpochId: %v)",
            nodeId,
            address,
            node->Status().EpochId().Load());

        return node;
    }

    TError ProcessHeartbeat(
        const TTransactionPtr& transaction,
        TNode* node,
        const TEpochId& epochId,
        ui64 sequenceNumber,
        const TReqHeartbeat* request,
        TRspHeartbeat* response)
    {
        node->Status().EpochId().ScheduleLoad();
        node->Status().HeartbeatSequenceNumber().ScheduleLoad();
        node->Pods().ScheduleLoad();
        node->Resources().ScheduleLoad();

        auto prevSequenceNumber = node->Status().HeartbeatSequenceNumber().Load();
        if (sequenceNumber <= prevSequenceNumber) {
            THROW_ERROR_EXCEPTION("Wrong heartbeat sequence number: expected >%v, got %v",
                prevSequenceNumber,
                sequenceNumber)
                << TErrorAttribute("node_id", node->GetId())
                << TErrorAttribute("epoch_id", epochId);
        }

        if (node->Status().EpochId().Load() != epochId) {
            THROW_ERROR_EXCEPTION("Wrong heartbeat epoch: expected %v, got %v",
                node->Status().EpochId().Load(),
                epochId)
                << TErrorAttribute("node_id", node->GetId());
        }

        THashMap<TObjectId, TPod*> expectedSpecPods;
        for (auto* pod : node->Pods().Load()) {
            YCHECK(expectedSpecPods.emplace(pod->GetId(), pod).second);
            pod->Spec().UpdateTimestamp().ScheduleLoad();
        }

        NLogging::TLogger Logger(this->Logger);
        Logger.AddTag("NodeId: %v",
            node->GetId());

        LOG_DEBUG("Heartbeat received (EpochId: %v, SequenceNumber: %v)",
            epochId,
            sequenceNumber);

        auto now = TInstant::Now();

        node->Status().HeartbeatSequenceNumber() = sequenceNumber;
        node->Status().LastSeenTime() = now;

        for (auto* resource : node->Resources().Load()) {
            resource->Status().ActualAllocations().ScheduleLoad();
        }

        std::vector<TPod*> podsToUpdate;
        std::vector<TPod*> podsToKeep;
        std::vector<TObjectId> podIdsToRemove;

        THashSet<TObjectId> reportedPodIds;
        THashSet<TObjectId> upToDatePodIds;

        // Schedule preload.
        for (const auto& podEntry : request->pods()) {
            auto podId = FromProto<TObjectId>(podEntry.pod_id());
            auto* pod = transaction->GetPod(podId);
            pod->ScheduleTombstoneCheck();
        }

        // Check for unknown pods.
        node->Status().Other()->clear_unknown_pod_ids();
        for (const auto& podEntry : request->pods()) {
            auto podId = FromProto<TObjectId>(podEntry.pod_id());
            auto* pod = transaction->GetPod(podId);
            if (!pod->DoesExist() && !pod->IsTombstone()) {
                LOG_DEBUG("Unknown pod reported by agent (PodId: %v)",
                    podId);
                node->Status().Other()->add_unknown_pod_ids(podId);
            }
        }

        if (node->Status().Other()->unknown_pod_ids_size() > 0 &&
            !node->Spec().Load().force_remove_unknown_pods())
        {
            return TError("Unknown pods found at node, heartbeat ignored");
        }

        // Actually examine pods from the heartbeat.
        for (const auto& podEntry : request->pods()) {
            auto podId = FromProto<TObjectId>(podEntry.pod_id());

            auto currentState = static_cast<EPodCurrentState>(podEntry.current_state());
            // TODO(babenko): remove after agents are updated
            if (currentState == EPodCurrentState::Unknown) {
                currentState = static_cast<EPodCurrentState>(podEntry.status().current_state());
            }

            YCHECK(reportedPodIds.insert(podId).second);

            auto podIt = expectedSpecPods.find(podId);
            if (podIt == expectedSpecPods.end()) {
                if (currentState == EPodCurrentState::Stopped) {
                    LOG_DEBUG("Unexpected pod is stopped, ignored (PodId: %v)",
                        podId);
                } else {
                    LOG_DEBUG("Unexpected pod found, removal scheduled (PodId: %v, CurrentState: %v)",
                        podId,
                        currentState);
                    podIdsToRemove.push_back(podId);
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
                        node->GetId(),
                        podId,
                        agentTimestamp,
                        masterTimestamp);
                }

                if (agentTimestamp < masterTimestamp) {
                    LOG_DEBUG("Sending pod spec update (PodId: %v, CurrentState: %v, SpecTimestamp: %llx -> %llx)",
                        podId,
                        currentState,
                        agentTimestamp,
                        masterTimestamp);
                    pod->Spec().IssPayload().ScheduleLoad();
                    pod->Spec().PodAgentPayload().ScheduleLoad();
                    podsToUpdate.push_back(pod);
                } else {
                    podsToKeep.push_back(pod);
                    if (currentState == EPodCurrentState::Started) {
                        YCHECK(upToDatePodIds.insert(podId).second);
                    }
                }

                if (podEntry.has_status()) {
                    LOG_DEBUG("Pod status update received (PodId: %v, CurrentState: %v)",
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

                pod->Status().Agent().Other()->set_last_heartbeat_time(ToProto<ui64>(now));

                pod->Status().AgentSpecTimestamp() = agentTimestamp;
            }
        }

        for (const auto& pair : expectedSpecPods) {
            const auto& podId = pair.first;
            auto* pod = pair.second;
            if (reportedPodIds.find(podId) == reportedPodIds.end()) {
                LOG_DEBUG("Requesting pod install (PodId: %v, SpecTimestamp: %llx)",
                    podId,
                    pod->Spec().UpdateTimestamp().Load());
                podsToUpdate.push_back(pod);
            }
        }

        for (auto* pod : podsToUpdate) {
            auto* podEntry = response->add_pods();
            ToProto(podEntry->mutable_pod_id(), pod->GetId());
            podEntry->set_spec_timestamp(pod->Spec().UpdateTimestamp().Load());
            podEntry->mutable_spec()->set_target_state(NClient::NApi::NProto::PTS_ACTIVE);
            podEntry->set_target_state(NClient::NApi::NProto::PTS_ACTIVE);
            PopulateAgentSpec(podEntry->mutable_spec(), node, pod);
        }

        for (auto* pod : podsToKeep) {
            auto* podEntry = response->add_pods();
            ToProto(podEntry->mutable_pod_id(), pod->GetId());
            podEntry->set_spec_timestamp(pod->Spec().UpdateTimestamp().Load());
            podEntry->set_target_state(NClient::NApi::NProto::PTS_ACTIVE);
        }

        for (const auto& podId : podIdsToRemove) {
            auto* podEntry = response->add_pods();
            ToProto(podEntry->mutable_pod_id(), podId);
            podEntry->mutable_spec()->set_target_state(NClient::NApi::NProto::PTS_REMOVED);
            podEntry->set_target_state(NClient::NApi::NProto::PTS_REMOVED);
        }

        for (auto* resource : node->Resources().Load()) {
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
                            reportedPodIds.find(podId) == reportedPodIds.end() ||
                            upToDatePodIds.find(podId) != upToDatePodIds.end();
                    }),
                actualAllocations->end());

            // Copy scheduled allocations for the up-to-date pods to the actual ones.
            for (const auto& scheduledAllocation : scheduledAllocations) {
                auto podId = FromProto<TObjectId>(scheduledAllocation.pod_id());
                if (upToDatePodIds.find(podId) == upToDatePodIds.end()) {
                    continue;
                }
                actualAllocations->emplace_back();
                actualAllocations->back().MergeFrom(scheduledAllocation);
            }
        }

        return {};
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

        LOG_DEBUG("Agent notification enqueued (NodeId: %v, Address: %v)",
            entry.Id,
            entry.Address);

        AgentNotificationQueue_.emplace(std::move(entry));
    }

private:
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

            LOG_DEBUG("Sending agent notification (NodeId: %v, Address: %v)",
                entry.Id,
                entry.Address);

            auto proxy = CreateAgentProxy(entry.Address);
            auto req = proxy->Notify();
            return req->Invoke().Subscribe(BIND([=, this_ = MakeStrong(this)] (const TAgentServiceProxy::TErrorOrRspNotifyPtr& rspOrError) {
                if (rspOrError.IsOK()) {
                    LOG_DEBUG("Agent notification succeeded (NodeId: %v, Address: %v)",
                        entry.Id,
                        entry.Address);
                } else {
                    LOG_DEBUG(rspOrError, "Agent notification failed (NodeId: %v, Address: %v)",
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

    void PopulateAgentSpec(NClient::NNodes::NProto::TPodSpec* protoSpec, TNode* node, TPod* pod)
    {
        const auto& podSpec = pod->Spec();
        const auto& podStatus = pod->Status();

        // Schedule loading some properties to be used later on.
        podSpec.Other().ScheduleLoad();
        podStatus.Other().ScheduleLoad();
        node->Resources().ScheduleLoad();

        auto* cpuResource = node->GetCpuResourceOrThrow();
        auto properties = BuildPortoProperties(
            cpuResource->Spec().Load().cpu(),
            pod->Spec().Other().Load(),
            pod->Status().Other().Load());
        for (const auto& pair : properties) {
            auto* protoProperty = protoSpec->add_porto_properties();
            protoProperty->set_key(pair.first);
            protoProperty->set_value(pair.second);
            LOG_DEBUG("Setting Porto property (PodId: %v, Name: %v, Value: %v)",
                pod->GetId(),
                pair.first,
                pair.second);
        };

        // Payload
        // COMPAT(babenko)
        if (podSpec.IssPayload().Load()) {
            protoSpec->set_iss_payload(podSpec.IssPayload().Load());
        } else {
            *protoSpec->mutable_pod_agent_payload() = podSpec.PodAgentPayload().Load();
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
};

////////////////////////////////////////////////////////////////////////////////

TNodeTracker::TNodeTracker(TBootstrap* bootstrap, TNodeTrackerConfigPtr config)
    : Impl_(New<TImpl>(bootstrap, std::move(config)))
{ }

TNode* TNodeTracker::ProcessHandshake(
    const TTransactionPtr& transaction,
    const TObjectId& nodeId,
    const TString& address)
{
    return Impl_->ProcessHandshake(
        transaction,
        nodeId,
        address);
}

TError TNodeTracker::ProcessHeartbeat(
    const TTransactionPtr& transaction,
    TNode* node,
    const TEpochId& epochId,
    ui64 sequenceNumber,
    const TReqHeartbeat* request,
    TRspHeartbeat* response)
{
    return Impl_->ProcessHeartbeat(
        transaction,
        node,
        epochId,
        sequenceNumber,
        request,
        response);
}

void TNodeTracker::NotifyAgent(TNode* node)
{
    Impl_->NotifyAgent(node);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodes
} // namespace NServer
} // namespace NYP

