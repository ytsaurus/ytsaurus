#include "node_tracker.h"
#include "porto.h"
#include "private.h"

#include <yp/server/objects/transaction.h>
#include <yp/server/objects/node.h>
#include <yp/server/objects/pod.h>
#include <yp/server/objects/resource.h>

#include <yp/server/master/config.h>

#include <yp/client/nodes/node_tracker_service_proxy.h>
#include <yp/client/nodes/agent_service_proxy.h>

#include <yt/core/rpc/caching_channel_factory.h>

#include <yt/core/rpc/grpc/channel.h>

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

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TNodeTracker::TImpl
    : public TRefCounted
{
public:
    TImpl(TBootstrap* bootstrap, TNodeTrackerConfigPtr config)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
    { }

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

    void ProcessHeartbeat(
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

        node->Status().HeartbeatSequenceNumber() = sequenceNumber;
        node->Status().LastSeenTime() = TInstant::Now();

        for (auto* resource : node->Resources().Load()) {
            resource->Status().ActualAllocations().ScheduleLoad();
        }

        std::vector<TPod*> podsToUpdate;
        std::vector<TPod*> podsToKeep;
        std::vector<TObjectId> podIdsToRemove;

        THashSet<TObjectId> reportedPodIds;
        THashSet<TObjectId> upToDatePodIds;

        for (const auto& podEntry : request->pods()) {
            auto currentState = static_cast<EPodCurrentState>(podEntry.status().current_state());
            auto podId = FromProto<TObjectId>(podEntry.pod_id());

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

                if (agentTimestamp > masterTimestamp) {
                    THROW_ERROR_EXCEPTION("Node %Qv has pod %Qv with spec revision %llx while only revision %llx is available at master",
                        node->GetId(),
                        podId,
                        agentTimestamp,
                        masterTimestamp);
                }

                if (agentTimestamp < masterTimestamp) {
                    LOG_DEBUG("Sending pod spec update (PodId: %v, SpecRevision: %llx -> %llx)",
                        podId,
                        agentTimestamp,
                        masterTimestamp);
                    pod->Spec().IssPayload().ScheduleLoad();
                    podsToUpdate.push_back(pod);
                } else {
                    podsToKeep.push_back(pod);
                    if (currentState == EPodCurrentState::Started) {
                        YCHECK(upToDatePodIds.insert(podId).second);
                    }
                }

                if (currentState != EPodCurrentState::Unknown) {
                    LOG_DEBUG("Pod status update received (PodId: %v, CurrentState: %v)",
                        podId,
                        currentState);
                    pod->Status().Agent().State() = currentState;
                    pod->Status().Agent().IssPayload() = podEntry.status().iss_payload();
                }

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
            PopulateAgentSpec(podEntry->mutable_spec(), node, pod);
        }

        for (auto* pod : podsToKeep) {
            auto* podEntry = response->add_pods();
            ToProto(podEntry->mutable_pod_id(), pod->GetId());
            podEntry->set_spec_timestamp(pod->Spec().UpdateTimestamp().Load());
            podEntry->mutable_spec()->set_target_state(NClient::NApi::NProto::PTS_ACTIVE);
        }

        for (const auto& podId : podIdsToRemove) {
            auto* podEntry = response->add_pods();
            ToProto(podEntry->mutable_pod_id(), podId);
            podEntry->mutable_spec()->set_target_state(NClient::NApi::NProto::PTS_REMOVED);
        }

        THashMap<TObjectId, size_t> podIdToActualAllocationIndex;

        for (auto* resource : node->Resources().Load()) {
            const auto& scheduledAllocations = resource->Status().ScheduledAllocations().Load();
            auto* actualAllocations = resource->Status().ActualAllocations().Get();

            actualAllocations->erase(
                std::remove_if(
                    actualAllocations->begin(),
                    actualAllocations->end(),
                    [&] (const auto& allocation) {
                        auto podId = FromProto<TObjectId>(allocation.pod_id());
                        return reportedPodIds.find(podId) == reportedPodIds.end();
                    }),
                actualAllocations->end());

            podIdToActualAllocationIndex.clear();
            for (size_t index = 0; index < actualAllocations->size(); ++index) {
                const auto& allocation = (*actualAllocations)[index];
                YCHECK(podIdToActualAllocationIndex.emplace(
                    FromProto<TObjectId>(allocation.pod_id()),
                    index).second);
            }

            for (const auto& scheduledAllocation : scheduledAllocations) {
                auto podId = FromProto<TObjectId>(scheduledAllocation.pod_id());
                if (upToDatePodIds.find(podId) == upToDatePodIds.end()) {
                    continue;
                }
                auto it = podIdToActualAllocationIndex.find(podId);
                if (it == podIdToActualAllocationIndex.end()) {
                    actualAllocations->push_back(scheduledAllocation);
                } else {
                    (*actualAllocations)[it->second].MergeFrom(scheduledAllocation);
                }
            }
        }
    }

    void NotifyAgent(TNode* node)
    {
        if (!Config_->EnableAgentNotification) {
            return;
        }

        auto nodeId = node->GetId();
        auto address = node->Status().AgentAddress().Load();

        LOG_DEBUG("Sending agent notification (NodeId: %v, Address: %v)",
            nodeId,
            address);

        auto proxy = CreateAgentProxy(address);
        auto req = proxy->Notify();
        return req->Invoke().Subscribe(BIND([=, this_ = MakeStrong(this)] (const TAgentServiceProxy::TErrorOrRspNotifyPtr& rspOrError) {
            if (rspOrError.IsOK()) {
                LOG_DEBUG("Agent notification succeeded (NodeId: %v, Address: %v)",
                    nodeId,
                    address);
            } else {
                LOG_DEBUG(rspOrError, "Agent notification failed (NodeId: %v, Address: %v)",
                    nodeId,
                    address);
            }
        }));
    }

private:
    TBootstrap* const Bootstrap_;
    const TNodeTrackerConfigPtr Config_;

    const IChannelFactoryPtr NodeChannelFactory_ = CreateCachingChannelFactory(GetGrpcChannelFactory());

    const NLogging::TLogger& Logger = NNodes::Logger;


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

        auto properties = BuildPortoProperties(node->Spec().Load(), pod->Spec().Other().Load(), pod->Status().Other().Load());
        for (const auto& pair : properties) {
            auto* protoProperty = protoSpec->add_porto_properties();
            protoProperty->set_key(pair.first);
            protoProperty->set_value(pair.second);
            LOG_DEBUG("Setting Porto property (PodId: %v, Name: %v, Value: %v)",
                pod->GetId(),
                pair.first,
                pair.second);
        };

        // ISS payload
        protoSpec->set_iss_payload(podSpec.IssPayload());

        // Copy some fields from pod status/spec.
        const auto& specOther = pod->Spec().Other().Load();
        const auto& statusOther = pod->Status().Other().Load();
        protoSpec->mutable_ip6_address_requests()->CopyFrom(specOther.ip6_address_requests());
        protoSpec->mutable_ip6_subnet_requests()->CopyFrom(specOther.ip6_subnet_requests());
        protoSpec->mutable_ip6_address_allocations()->CopyFrom(statusOther.ip6_address_allocations());
        protoSpec->mutable_ip6_subnet_allocations()->CopyFrom(statusOther.ip6_subnet_allocations());
        protoSpec->mutable_dns()->CopyFrom(statusOther.dns());
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

void TNodeTracker::ProcessHeartbeat(
    const NObjects::TTransactionPtr& transaction,
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

void TNodeTracker::NotifyAgent(TNode* node)
{
    Impl_->NotifyAgent(node);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodes
} // namespace NServer
} // namespace NYP

