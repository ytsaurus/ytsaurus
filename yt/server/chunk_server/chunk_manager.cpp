#include "stdafx.h"
#include "chunk_manager.h"
#include "config.h"
#include "node.h"
#include "chunk.h"
#include "chunk_list.h"
#include "job.h"
#include "job_list.h"
#include "chunk_placement.h"
#include "chunk_replicator.h"
#include "node_lease_tracker.h"
#include "node_statistics.h"
#include "chunk_service_proxy.h"
#include "node_authority.h"
#include "node_statistics.h"
#include "chunk_tree_balancer.h"
#include "private.h"

#include <ytlib/misc/foreach.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/misc/guid.h>
#include <ytlib/misc/id_generator.h>
#include <ytlib/misc/address.h>
#include <ytlib/misc/string.h>

#include <ytlib/codecs/codec.h>

#include <ytlib/chunk_client/chunk_ypath.pb.h>
#include <ytlib/chunk_client/chunk_list_ypath.pb.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/table_client/table_ypath.pb.h>
#include <ytlib/table_client/schema.h>

#include <ytlib/meta_state/meta_state_manager.h>
#include <ytlib/meta_state/composite_meta_state.h>
#include <ytlib/meta_state/map.h>

#include <ytlib/ytree/fluent.h>

#include <ytlib/logging/log.h>

#include <server/chunk_server/chunk_manager.pb.h>
#include <server/chunk_server/chunk_manager.pb.h>

#include <server/cypress_server/cypress_manager.h>

#include <server/cell_master/load_context.h>
#include <server/cell_master/bootstrap.h>
#include <server/cell_master/meta_state_facade.h>

#include <server/transaction_server/transaction_manager.h>
#include <server/transaction_server/transaction.h>

#include <server/object_server/type_handler_detail.h>

namespace NYT {
namespace NChunkServer {

using namespace NRpc;
using namespace NMetaState;
using namespace NTransactionServer;
using namespace NTransactionClient;
using namespace NObjectServer;
using namespace NObjectClient;
using namespace NYTree;
using namespace NCellMaster;
using namespace NCypressServer;
using namespace NChunkClient::NProto;
using namespace NChunkServer::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TChunkTypeHandler
    : public TObjectTypeHandlerBase<TChunk>
{
public:
    explicit TChunkTypeHandler(TImpl* owner);

    virtual EObjectType GetType() override
    {
        return EObjectType::Chunk;
    }

    virtual TObjectId Create(
        TTransaction* transaction,
        TReqCreateObject* request,
        TRspCreateObject* response) override;

    virtual IObjectProxyPtr GetProxy(
        const TObjectId& id,
        TTransaction* transaction) override;

private:
    TImpl* Owner;

    virtual void DoDestroy(TChunk* chunk) override;

};

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TChunkListTypeHandler
    : public TObjectTypeHandlerBase<TChunkList>
{
public:
    explicit TChunkListTypeHandler(TImpl* owner);

    virtual EObjectType GetType() override
    {
        return EObjectType::ChunkList;
    }

    virtual TObjectId Create(
        TTransaction* transaction,
        TReqCreateObject* request,
        TRspCreateObject* response) override;

    virtual IObjectProxyPtr GetProxy(
        const TObjectId& id,
        TTransaction* transaction) override;

private:
    TImpl* Owner;

    virtual void DoDestroy(TChunkList* chunkList) override;

};

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TImpl
    : public NMetaState::TMetaStatePart
{
public:
    TImpl(
        TChunkManagerConfigPtr config,
        TBootstrap* bootstrap)
        : TMetaStatePart(
            bootstrap->GetMetaStateFacade()->GetManager(),
            bootstrap->GetMetaStateFacade()->GetState())
        , Config(config)
        , Bootstrap(bootstrap)
        , ChunkTreeBalancer(Bootstrap, Config->ChunkTreeBalancer)
        , ChunkReplicaCount(0)
        , Profiler(ChunkServerProfiler)
        , AddChunkCounter("/add_chunk_rate")
        , RemoveChunkCounter("/remove_chunk_rate")
        , AddChunkReplicaCounter("/add_chunk_replica_rate")
        , RemoveChunkReplicaCounter("/remove_chunk_replica_rate")
        , StartJobCounter("/start_job_rate")
        , StopJobCounter("/stop_job_rate")
    {
        YCHECK(config);
        YCHECK(bootstrap);

        RegisterMethod(BIND(&TImpl::FullHeartbeat, Unretained(this)));
        RegisterMethod(BIND(&TImpl::IncrementalHeartbeat, Unretained(this)));
        RegisterMethod(BIND(&TImpl::UpdateJobs, Unretained(this)));
        RegisterMethod(BIND(&TImpl::RegisterNode, Unretained(this)));
        RegisterMethod(BIND(&TImpl::UnregisterNode, Unretained(this)));
        RegisterMethod(BIND(&TImpl::RebalanceChunkTree, Unretained(this)));

        TLoadContext context(bootstrap);

        auto metaState = bootstrap->GetMetaStateFacade()->GetState();
        metaState->RegisterLoader(
            "ChunkManager.Keys.1",
            BIND(&TImpl::LoadKeys, MakeStrong(this)));
        metaState->RegisterLoader(
            "ChunkManager.Values.1",
            BIND(&TImpl::LoadValues, MakeStrong(this), context));
        metaState->RegisterSaver(
            "ChunkManager.Keys.1",
            BIND(&TImpl::SaveKeys, MakeStrong(this)),
            ESavePhase::Keys);
        metaState->RegisterSaver(
            "ChunkManager.Values.1",
            BIND(&TImpl::SaveValues, MakeStrong(this)),
            ESavePhase::Values);

        metaState->RegisterPart(this);

        auto objectManager = bootstrap->GetObjectManager();
        objectManager->RegisterHandler(New<TChunkTypeHandler>(this));
        objectManager->RegisterHandler(New<TChunkListTypeHandler>(this));
    }

    TMutationPtr CreateRegisterNodeMutation(
        const TMetaReqRegisterNode& request)
    {
        return Bootstrap
            ->GetMetaStateFacade()
            ->CreateMutation(this, request, &TThis::RegisterNode);
    }

    TMutationPtr CreateUnregisterNodeMutation(
        const TMetaReqUnregisterNode& request)
    {
        return Bootstrap
            ->GetMetaStateFacade()
            ->CreateMutation(this, request, &TThis::UnregisterNode);
    }

    TMutationPtr CreateFullHeartbeatMutation(
        TCtxFullHeartbeatPtr context)
    {
        return Bootstrap
            ->GetMetaStateFacade()
            ->CreateMutation(EStateThreadQueue::Heartbeat)
            ->SetRequestData(context->GetUntypedContext()->GetRequestBody())
            ->SetType(context->Request().GetTypeName())
            ->SetAction(BIND(&TThis::FullHeartbeatWithContext, MakeStrong(this), context));
    }

    TMutationPtr CreateIncrementalHeartbeatMutation(
        const TMetaReqIncrementalHeartbeat& request)
    {
        return Bootstrap
            ->GetMetaStateFacade()
            ->CreateMutation(this, request, &TThis::IncrementalHeartbeat, EStateThreadQueue::Heartbeat);
    }

    TMutationPtr CreateUpdateJobsMutation(
        const TMetaReqUpdateJobs& request)
    {
        return Bootstrap
            ->GetMetaStateFacade()
            ->CreateMutation(this, request, &TThis::UpdateJobs);
    }

    TMutationPtr CreateRebalanceChunkTreeMutation(
        const TMetaReqRebalanceChunkTree& request)
    {
        return Bootstrap
            ->GetMetaStateFacade()
            ->CreateMutation(this, request, &TThis::RebalanceChunkTree);
    }


    DECLARE_METAMAP_ACCESSORS(Chunk, TChunk, TChunkId);
    DECLARE_METAMAP_ACCESSORS(ChunkList, TChunkList, TChunkListId);
    DECLARE_METAMAP_ACCESSORS(Node, TDataNode, TNodeId);
    DECLARE_METAMAP_ACCESSORS(JobList, TJobList, TChunkId);
    DECLARE_METAMAP_ACCESSORS(Job, TJob, TJobId);

    DEFINE_SIGNAL(void(const TDataNode*), NodeRegistered);
    DEFINE_SIGNAL(void(const TDataNode*), NodeUnregistered);


    TDataNode* FindNodeByAddresss(const Stroka& address)
    {
        auto it = NodeAddressMap.find(address);
        return it == NodeAddressMap.end() ? NULL : it->second;
    }

    TDataNode* FindNodeByHostName(const Stroka& hostName)
    {
        auto it = NodeHostNameMap.find(hostName);
        return it == NodeAddressMap.end() ? NULL : it->second;
    }

    const TReplicationSink* FindReplicationSink(const Stroka& address)
    {
        auto it = ReplicationSinkMap.find(address);
        return it == ReplicationSinkMap.end() ? NULL : &it->second;
    }

    std::vector<TDataNode*> AllocateUploadTargets(
        int nodeCount,
        TNullable<Stroka> preferredHostName)
    {
        auto nodes = ChunkPlacement->GetUploadTargets(
            nodeCount,
            NULL,
            preferredHostName.GetPtr());
        FOREACH (auto* node, nodes) {
            ChunkPlacement->OnSessionHinted(node);
        }
        return nodes;
    }

    TChunk* CreateChunk()
    {
        Profiler.Increment(AddChunkCounter);
        auto id = Bootstrap->GetObjectManager()->GenerateId(EObjectType::Chunk);
        auto* chunk = new TChunk(id);
        ChunkMap.Insert(id, chunk);
        return chunk;
    }

    TChunkList* CreateChunkList()
    {
        auto id = Bootstrap->GetObjectManager()->GenerateId(EObjectType::ChunkList);
        auto* chunkList = new TChunkList(id);
        ChunkListMap.Insert(id, chunkList);
        return chunkList;
    }


    void AttachToChunkList(
        TChunkList* chunkList,
        const TChunkTreeRef* childrenBegin,
        const TChunkTreeRef* childrenEnd)
    {
        auto objectManager = Bootstrap->GetObjectManager();

        TChunkTreeStatistics accumulatedDelta;
        for (auto it = childrenBegin; it != childrenEnd; ++it) {
            auto childRef = *it;
            if (!chunkList->Children().empty()) {
                chunkList->RowCountSums().push_back(
                    chunkList->Statistics().RowCount +
                    accumulatedDelta.RowCount);
            }
            chunkList->Children().push_back(childRef);
            SetChunkTreeParent(chunkList, childRef);
            objectManager->RefObject(childRef);

            TChunkTreeStatistics delta;
            switch (childRef.GetType()) {
                case EObjectType::Chunk:
                    delta = childRef.AsChunk()->GetStatistics();
                    break;
                case EObjectType::ChunkList:
                    delta = childRef.AsChunkList()->Statistics();
                    break;
                default:
                    YUNREACHABLE();
            }
            accumulatedDelta.Accumulate(delta);
        }

        UpdateStatistics(chunkList, accumulatedDelta);
        ScheduleChunkTreeRebalanceIfNeeded(chunkList);
    }

    void ScheduleChunkTreeRebalanceIfNeeded(TChunkList* chunkList)
    {
        TMetaReqRebalanceChunkTree request;
        if (IsLeader() &&
            ChunkTreeBalancer.CheckRebalanceNeeded(chunkList, &request))
        {
            // Don't retry in case of failure.
            // Balancing will happen eventually.
            CreateRebalanceChunkTreeMutation(request)->PostCommit();
        }
    }

    void AttachToChunkList(
        TChunkList* chunkList,
        const std::vector<TChunkTreeRef>& children)
    {
        AttachToChunkList(
            chunkList,
            &*children.begin(),
            &*children.begin() + children.size());
    }

    void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTreeRef childRef)
    {
        AttachToChunkList(chunkList, &childRef, &childRef + 1);
    }


    void ClearChunkList(TChunkList* chunkList)
    {
        // TODO(babenko): currently we only support clearing a chunklist with no parents.
        YCHECK(chunkList->Parents().empty());

        auto objectManager = Bootstrap->GetObjectManager();
        FOREACH (auto childRef, chunkList->Children()) {
            ResetChunkTreeParent(chunkList, childRef);
            objectManager->UnrefObject(childRef.GetId());
        }
        chunkList->Children().clear();
        chunkList->RowCountSums().clear();
        chunkList->Statistics() = TChunkTreeStatistics();

        LOG_DEBUG_UNLESS(IsRecovery(),"Chunk list cleared (ChunkListId: %s)", ~chunkList->GetId().ToString());
    }

    void SetChunkTreeParent(TChunkList* parent, TChunkTreeRef childRef)
    {
        switch (childRef.GetType()) {
            case EObjectType::Chunk:
                childRef.AsChunk()->Parents().push_back(parent);
                break;
            case EObjectType::ChunkList:
                childRef.AsChunkList()->Parents().insert(parent);
                break;
            default:
                YUNREACHABLE();
        }
    }

    void ResetChunkTreeParent(TChunkList* parent, TChunkTreeRef childRef)
    {
        switch (childRef.GetType()) {
            case EObjectType::Chunk: {
                auto& parents = childRef.AsChunk()->Parents();
                auto it = std::find(parents.begin(), parents.end(), parent);
                YASSERT(it != parents.end());
                parents.erase(it);
                break;
            }
            case EObjectType::ChunkList: {
                auto& parents = childRef.AsChunkList()->Parents();
                auto it = parents.find(parent);
                YASSERT(it != parents.end());
                parents.erase(it);
                break;
            }
            default:
                YUNREACHABLE();
        }
    }



    void ScheduleJobs(
        TDataNode* node,
        const std::vector<TJobInfo>& runningJobs,
        std::vector<TJobStartInfo>* jobsToStart,
        std::vector<TJobStopInfo>* jobsToStop)
    {
        ChunkReplicator->ScheduleJobs(
            node,
            runningJobs,
            jobsToStart,
            jobsToStop);
    }

    const yhash_set<TChunkId>& LostChunkIds() const;
    const yhash_set<TChunkId>& OverreplicatedChunkIds() const;
    const yhash_set<TChunkId>& UnderreplicatedChunkIds() const;

    void FillNodeAddresses(
        ::google::protobuf::RepeatedPtrField<TProtoStringType>* addresses,
        const TChunk* chunk)
    {
        FOREACH (auto nodeId, chunk->StoredLocations()) {
            const auto* node = GetNode(nodeId);
            addresses->Add()->assign(node->GetAddress());
        }

        if (~chunk->CachedLocations()) {
            FOREACH (auto nodeId, *chunk->CachedLocations()) {
                const auto* node = GetNode(nodeId);
                addresses->Add()->assign(node->GetAddress());
            }
        }
    }

    TTotalNodeStatistics GetTotalNodeStatistics()
    {
        TTotalNodeStatistics result;
        auto keys = NodeMap.GetKeys();
        FOREACH (const auto& key, keys) {
            const auto* node = NodeMap.Get(key);
            const auto& statistics = node->Statistics();
            result.AvailbaleSpace += statistics.available_space();
            result.UsedSpace += statistics.used_space();
            result.ChunkCount += statistics.chunk_count();
            result.SessionCount += statistics.session_count();
            result.OnlineNodeCount++;
        }
        return result;
    }

    bool IsNodeConfirmed(const TDataNode* node)
    {
        return NodeLeaseTracker->IsNodeConfirmed(node);
    }

    i32 GetChunkReplicaCount()
    {
        return ChunkReplicaCount;
    }

    bool IsReplicatorEnabled()
    {
        return ChunkReplicator->IsEnabled();
    }

    TChunkTreeRef GetChunkTree(const TChunkTreeId& id)
    {
        auto type = TypeFromId(id);
        if (type == EObjectType::Chunk) {
            auto* chunk = FindChunk(id);
            if (!chunk) {
                THROW_ERROR_EXCEPTION("No such chunk %s", ~id.ToString());
            }
            return TChunkTreeRef(chunk);
        } else if (type == EObjectType::ChunkList) {
            auto* chunkList = FindChunkList(id);
            if (!chunkList) {
                THROW_ERROR_EXCEPTION("No such chunkList %s", ~id.ToString());
            }
            return TChunkTreeRef(chunkList);
        } else {
            THROW_ERROR_EXCEPTION("Invalid type of object %s", ~id.ToString());
        }
    }


private:
    typedef TImpl TThis;
    friend class TChunkTypeHandler;
    friend class TChunkProxy;
    friend class TChunkListTypeHandler;
    friend class TChunkListProxy;

    TChunkManagerConfigPtr Config;
    TBootstrap* Bootstrap;

    TChunkTreeBalancer ChunkTreeBalancer;
    
    i32 ChunkReplicaCount;

    NProfiling::TProfiler& Profiler;
    NProfiling::TRateCounter AddChunkCounter;
    NProfiling::TRateCounter RemoveChunkCounter;
    NProfiling::TRateCounter AddChunkReplicaCounter;
    NProfiling::TRateCounter RemoveChunkReplicaCounter;
    NProfiling::TRateCounter StartJobCounter;
    NProfiling::TRateCounter StopJobCounter;

    TChunkPlacementPtr ChunkPlacement;
    TChunkReplicatorPtr ChunkReplicator;
    TNodeLeaseTrackerPtr NodeLeaseTracker;
    
    TIdGenerator<TNodeId> NodeIdGenerator;

    TMetaStateMap<TChunkId, TChunk> ChunkMap;
    TMetaStateMap<TChunkListId, TChunkList> ChunkListMap;

    TMetaStateMap<TNodeId, TDataNode> NodeMap;
    yhash_map<Stroka, TDataNode*> NodeAddressMap;
    yhash_multimap<Stroka, TDataNode*> NodeHostNameMap;

    TMetaStateMap<TChunkId, TJobList> JobListMap;
    TMetaStateMap<TJobId, TJob> JobMap;

    yhash_map<Stroka, TReplicationSink> ReplicationSinkMap;

    void UpdateStatistics(TChunkList* chunkList, const TChunkTreeStatistics& statisticsDelta)
    {
        // Go upwards and apply delta.
        // Also reset Sorted flags.
        // Check that parents are unique along the way.
        auto statisticsCopy = statisticsDelta;
        while (true) {
            ++statisticsCopy.Rank;

            chunkList->Statistics().Accumulate(statisticsCopy);
            chunkList->SortedBy().clear();

            const auto& parents = chunkList->Parents();
            if (parents.empty()) {
                break;
            }

            YCHECK(parents.size() == 1);
            chunkList = *parents.begin();
        }
    }


    void DestroyChunk(TChunk* chunk)
    {
        auto chunkId = chunk->GetId();

        // Unregister chunk replicas from all known locations.
        FOREACH (auto nodeId, chunk->StoredLocations()) {
            ScheduleChunkReplicaRemoval(nodeId, chunk, false);
        }
        if (~chunk->CachedLocations()) {
            FOREACH (auto nodeId, *chunk->CachedLocations()) {
                ScheduleChunkReplicaRemoval(nodeId, chunk, true);
            }
        }

        // Remove all associated jobs.
        auto* jobList = FindJobList(chunkId);
        if (jobList) {
            FOREACH (auto job, jobList->Jobs()) {
                // Suppress removal from job list.
                RemoveJob(job, true, false);
            }
            JobListMap.Remove(chunkId);
        }

        // Notify the balancer about chunk's death.
        if (ChunkReplicator) {
            ChunkReplicator->OnChunkRemoved(chunk);
        }

        Profiler.Increment(RemoveChunkCounter);
    }

    void DestroyChunkList(TChunkList* chunkList)
    {
        auto objectManager = Bootstrap->GetObjectManager();
        // Drop references to children.
        FOREACH (auto childRef, chunkList->Children()) {
            ResetChunkTreeParent(chunkList, childRef);
            objectManager->UnrefObject(childRef.GetId());
        }
    }


    TMetaRspRegisterNode RegisterNode(const TMetaReqRegisterNode& request)
    {
        Stroka address = request.address();
        auto incarnationId = TIncarnationId::FromProto(request.incarnation_id());
        const auto& statistics = request.statistics();
    
        auto nodeId = NodeIdGenerator.Next();
    
        auto* existingNode = FindNodeByAddresss(address);
        if (existingNode) {
            LOG_INFO_UNLESS(IsRecovery(), "Node kicked out due to address conflict (Address: %s, NodeId: %d)",
                ~address,
                existingNode->GetId());
            DoUnregisterNode(existingNode);
        }

        LOG_INFO_UNLESS(IsRecovery(), "Node registered (Address: %s, NodeId: %d, IncarnationId: %s, %s)",
            ~address,
            nodeId,
            ~incarnationId.ToString(),
            ~ToString(statistics));

        auto* newNode = new TDataNode(
            nodeId,
            address,
            incarnationId);
        newNode->SetState(ENodeState::Registered);
        newNode->Statistics() = statistics;

        NodeMap.Insert(nodeId, newNode);
        NodeAddressMap.insert(MakePair(address, newNode));
        NodeHostNameMap.insert(MakePair(Stroka(GetServiceHostName(address)), newNode));

        if (IsLeader()) {
            ChunkPlacement->OnNodeRegistered(newNode);
            ChunkReplicator->OnNodeRegistered(newNode);
            StartNodeTracking(newNode, false);
        }

        TMetaRspRegisterNode response;
        response.set_node_id(nodeId);
        return response;
    }

    void UnregisterNode(const TMetaReqUnregisterNode& request)
    { 
        auto nodeId = request.node_id();

        // Allow nodeId to be invalid, just ignore such obsolete requests.
        auto* node = FindNode(nodeId);
        if (node) {
            DoUnregisterNode(node);
        }
    }

    void FullHeartbeatWithContext(TCtxFullHeartbeatPtr context)
    {
        return FullHeartbeat(context->Request());
    }

    void FullHeartbeat(const TMetaReqFullHeartbeat& request)
    {
        PROFILE_TIMING ("/full_heartbeat_time") {
            Profiler.Enqueue("/full_heartbeat_chunks", request.chunks_size());

            auto nodeId = request.node_id();
            const auto& statistics = request.statistics();

            auto* node = GetNode(nodeId);

            LOG_DEBUG_UNLESS(IsRecovery(), "Full heartbeat received (Address: %s, NodeId: %d, State: %s, %s, Chunks: %d)",
                ~node->GetAddress(),
                nodeId,
                ~node->GetState().ToString(),
                ~ToString(statistics),
                static_cast<int>(request.chunks_size()));

            YCHECK(node->GetState() == ENodeState::Registered);
            node->SetState(ENodeState::Online);
            node->Statistics() = statistics;

            if (IsLeader()) {
                NodeLeaseTracker->OnNodeOnline(node, false);
                ChunkPlacement->OnNodeUpdated(node);
            }

            LOG_INFO_UNLESS(IsRecovery(), "Node online (Address: %s, NodeId: %d)",
                ~node->GetAddress(),
                nodeId);

            YCHECK(node->StoredChunks().empty());
            YCHECK(node->CachedChunks().empty());

            FOREACH (const auto& chunkInfo, request.chunks()) {
                ProcessAddedChunk(node, chunkInfo, false);
            }
        }
    }

    void IncrementalHeartbeat(const TMetaReqIncrementalHeartbeat& request)
    {
        Profiler.Enqueue("/incremental_heartbeat_chunks_added", request.added_chunks_size());
        Profiler.Enqueue("/incremental_heartbeat_chunks_removed", request.removed_chunks_size());
        PROFILE_TIMING ("/incremental_heartbeat_time") {
            auto nodeId = request.node_id();
            const auto& statistics = request.statistics();

            auto* node = GetNode(nodeId);

            LOG_DEBUG_UNLESS(IsRecovery(), "Incremental heartbeat received (Address: %s, NodeId: %d, State: %s, %s, ChunksAdded: %d, ChunksRemoved: %d)",
                ~node->GetAddress(),
                nodeId,
                ~node->GetState().ToString(),
                ~ToString(statistics),
                static_cast<int>(request.added_chunks_size()),
                static_cast<int>(request.removed_chunks_size()));

            YCHECK(node->GetState() == ENodeState::Online);
            node->Statistics() = statistics;

            if (IsLeader()) {
                NodeLeaseTracker->OnNodeHeartbeat(node);
                ChunkPlacement->OnNodeUpdated(node);
            }

            FOREACH (const auto& chunkInfo, request.added_chunks()) {
                ProcessAddedChunk(node, chunkInfo, true);
            }

            FOREACH (const auto& chunkInfo, request.removed_chunks()) {
                ProcessRemovedChunk(node, chunkInfo);
            }

            std::vector<TChunk*> UnapprovedChunks(node->UnapprovedChunks().begin(), node->UnapprovedChunks().end());
            FOREACH (auto* chunk, UnapprovedChunks) {
                RemoveChunkReplica(node, chunk, false, ERemoveReplicaReason::Unapproved);
            }
            node->UnapprovedChunks().clear();
        }
    }

    void UpdateJobs(const TMetaReqUpdateJobs& request)
    {
        PROFILE_TIMING ("/update_jobs_time") {
            auto nodeId = request.node_id();
            auto* node = GetNode(nodeId);

            FOREACH (const auto& startInfo, request.started_jobs()) {
                AddJob(node, startInfo);
            }

            FOREACH (const auto& stopInfo, request.stopped_jobs()) {
                auto jobId = TJobId::FromProto(stopInfo.job_id());
                auto* job = FindJob(jobId);
                if (job) {
                    // Remove from both job list and node.
                    RemoveJob(job, true, true);
                }
            }

            LOG_DEBUG_UNLESS(IsRecovery(), "Node jobs updated (Address: %s, NodeId: %d, JobsStarted: %d, JobsStopped: %d)",
                ~node->GetAddress(),
                nodeId,
                static_cast<int>(request.started_jobs_size()),
                static_cast<int>(request.stopped_jobs_size()));
        }
    }

    void RebalanceChunkTree(const TMetaReqRebalanceChunkTree& request)
    {
        PROFILE_TIMING ("/chunk_tree_rebalance_time") {
            auto rootId = TChunkListId::FromProto(request.root_id());
            auto* root = FindChunkList(rootId);
            if (!root) {
                return;
            }

            LOG_DEBUG_UNLESS(IsRecovery(), "Chunk tree rebalancing started (RootId: %s)",
                ~rootId.ToString());

            if (ChunkTreeBalancer.RebalanceChunkTree(root, request)) {
                LOG_DEBUG_UNLESS(IsRecovery(), "Chunk tree rebalancing completed");
            } else {
                LOG_DEBUG_UNLESS(IsRecovery(), "Chunk tree rebalancing canceled");
            }
        }
    }


    void SaveKeys(TOutputStream* output) const
    {
        ChunkMap.SaveKeys(output);
        ChunkListMap.SaveKeys(output);
        NodeMap.SaveKeys(output);
        JobMap.SaveKeys(output);
        JobListMap.SaveKeys(output);
    }

    void SaveValues(TOutputStream* output) const
    {
        ::Save(output, NodeIdGenerator);

        ChunkMap.SaveValues(output);
        ChunkListMap.SaveValues(output);
        NodeMap.SaveValues(output);
        JobMap.SaveValues(output);
        JobListMap.SaveValues(output);
    }

    void LoadKeys(TInputStream* input)
    {
        ChunkMap.LoadKeys(input);
        ChunkListMap.LoadKeys(input);
        NodeMap.LoadKeys(input);
        JobMap.LoadKeys(input);
        JobListMap.LoadKeys(input);
    }

    void LoadValues(TLoadContext context, TInputStream* input)
    {
        ::Load(input, NodeIdGenerator);

        ChunkMap.LoadValues(context, input);
        ChunkListMap.LoadValues(context, input);
        NodeMap.LoadValues(context, input);
        JobMap.LoadValues(context, input);
        JobListMap.LoadValues(context, input);

        // Compute chunk replica count.
        ChunkReplicaCount = 0;
        FOREACH (const auto& pair, NodeMap) {
            const auto* node = pair.second;
            ChunkReplicaCount += node->StoredChunks().size();
            ChunkReplicaCount += node->CachedChunks().size();
        }

        // Reconstruct address maps.
        NodeAddressMap.clear();
        NodeHostNameMap.clear();
        FOREACH (const auto& pair, NodeMap) {
            auto* node = pair.second;
            const auto& address = node->GetAddress();
            YCHECK(NodeAddressMap.insert(MakePair(address, node)).second);
            NodeHostNameMap.insert(MakePair(Stroka(GetServiceHostName(address)), node));
        }

        // Reconstruct ReplicationSinkMap.
        ReplicationSinkMap.clear();
        FOREACH (auto& pair, JobMap) {
            RegisterReplicationSinks(pair.second);
        }
    }

    virtual void Clear() override
    {
        NodeIdGenerator.Reset();
        ChunkMap.Clear();
        ChunkReplicaCount = 0;
        ChunkListMap.Clear();
        NodeMap.Clear();
        JobMap.Clear();
        JobListMap.Clear();

        NodeAddressMap.clear();
        NodeHostNameMap.clear();

        ReplicationSinkMap.clear();
    }


    virtual void OnStartRecovery() override
    {
        Profiler.SetEnabled(false);
    }

    virtual void OnStopRecovery() override
    {
        Profiler.SetEnabled(true);
    }
    
    virtual void OnLeaderRecoveryComplete() override
    {
        NodeLeaseTracker = New<TNodeLeaseTracker>(Config, Bootstrap);
        ChunkPlacement = New<TChunkPlacement>(Config, Bootstrap);
        ChunkReplicator = New<TChunkReplicator>(Config, Bootstrap, ChunkPlacement, NodeLeaseTracker);

        FOREACH (auto& pair, NodeMap) {
            auto* node = pair.second;
            ChunkPlacement->OnNodeRegistered(node);
            ChunkReplicator->OnNodeRegistered(node);
        }

        PROFILE_TIMING ("/full_chunk_refresh_time") {
            LOG_INFO("Starting full chunk refresh");
            ChunkReplicator->RefreshAllChunks();
            LOG_INFO("Full chunk refresh completed");
        }
    }

    virtual void OnActiveQuorumEstablished() override
    {
        // Assign initial leases to nodes.
        // NB: Nodes will remain unconfirmed until the first heartbeat.
        FOREACH (const auto& pair, NodeMap) { 
            StartNodeTracking(pair.second, true);
        }
    }

    virtual void OnStopLeading() override
    {
        ChunkPlacement.Reset();
        ChunkReplicator.Reset();
        NodeLeaseTracker.Reset();
    }


    void StartNodeTracking(TDataNode* node, bool recovery)
    {
        NodeLeaseTracker->OnNodeRegistered(node, recovery);
        if (node->GetState() == ENodeState::Online) {
            NodeLeaseTracker->OnNodeOnline(node, recovery);
        }
        NodeRegistered_.Fire(node);
    }

    void StopNodeTracking(TDataNode* node)
    {
        NodeLeaseTracker->OnNodeUnregistered(node);
        NodeUnregistered_.Fire(node);
    }


    void DoUnregisterNode(TDataNode* node)
    { 
        PROFILE_TIMING ("/node_unregistration_time") {
            auto nodeId = node->GetId();

            LOG_INFO_UNLESS(IsRecovery(), "Node unregistered (Address: %s, NodeId: %d)",
                ~node->GetAddress(),
                nodeId);

            if (IsLeader()) {
                ChunkPlacement->OnNodeUnregistered(node);
                ChunkReplicator->OnNodeUnregistered(node);
                StopNodeTracking(node);
            }

            FOREACH (auto& chunk, node->StoredChunks()) {
                RemoveChunkReplica(node, chunk, false, ERemoveReplicaReason::Reset);
            }

            FOREACH (auto& chunk, node->CachedChunks()) {
                RemoveChunkReplica(node, chunk, true, ERemoveReplicaReason::Reset);
            }

            FOREACH (auto& job, node->Jobs()) {
                // Suppress removal of job from node.
                RemoveJob(job, false, true);
            }

            const auto& address = node->GetAddress();
            YCHECK(NodeAddressMap.erase(address) == 1);
            {
                auto hostNameRange = NodeHostNameMap.equal_range(Stroka(GetServiceHostName(address)));
                for (auto it = hostNameRange.first; it != hostNameRange.second; ++it) {
                    if (it->second == node) {
                        NodeHostNameMap.erase(it);
                        break;
                    }
                }
            }
            NodeMap.Remove(nodeId);
        }
    }


    DECLARE_ENUM(EAddReplicaReason,
        (IncrementalHeartbeat)
        (FullHeartbeat)
        (Confirmation)
    );

    void AddChunkReplica(TDataNode* node, TChunk* chunk, bool cached, EAddReplicaReason reason)
    {
        auto chunkId = chunk->GetId();
        auto nodeId = node->GetId();

        if (node->HasChunk(chunk, cached)) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Chunk replica is already added (ChunkId: %s, Cached: %s, Reason: %s, Address: %s, NodeId: %d)",
                ~chunkId.ToString(),
                ~FormatBool(cached),
                ~reason.ToString(),
                ~node->GetAddress(),
                nodeId);
            return;
        }

        node->AddChunk(chunk, cached);
        chunk->AddLocation(nodeId, cached);

        if (!IsRecovery()) {
            LOG_EVENT(
                Logger,
                reason == EAddReplicaReason::FullHeartbeat ? NLog::ELogLevel::Trace : NLog::ELogLevel::Debug,
                "Chunk replica added (ChunkId: %s, Cached: %s, Address: %s, NodeId: %d)",
                ~chunkId.ToString(),
                ~FormatBool(cached),
                ~node->GetAddress(),
                nodeId);
        }

        if (!cached && IsLeader()) {
            ChunkReplicator->ScheduleChunkRefresh(chunk->GetId());
        }

        if (reason == EAddReplicaReason::IncrementalHeartbeat || reason == EAddReplicaReason::Confirmation) {
            Profiler.Increment(AddChunkReplicaCounter);
        }
    }

    void ScheduleChunkReplicaRemoval(TNodeId nodeId, TChunk* chunk, bool cached)
    {
        auto chunkId = chunk->GetId();
        auto* node = GetNode(nodeId);
        node->RemoveChunk(chunk, cached);

        if (!cached && IsLeader()) {
            ChunkReplicator->ScheduleChunkRemoval(node, chunkId);
        }
    }

    DECLARE_ENUM(ERemoveReplicaReason,
        (IncrementalHeartbeat)
        (Unapproved)
        (Reset)
    );

    void RemoveChunkReplica(TDataNode* node, TChunk* chunk, bool cached, ERemoveReplicaReason reason)
    {
        auto chunkId = chunk->GetId();
        auto nodeId = node->GetId();

        if (reason == ERemoveReplicaReason::IncrementalHeartbeat && !node->HasChunk(chunk, cached)) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Chunk replica is already removed (ChunkId: %s, Cached: %s, Reason: %s, Address: %s, NodeId: %d)",
                ~chunkId.ToString(),
                ~FormatBool(cached),
                ~reason.ToString(),
                ~node->GetAddress(),
                nodeId);
            return;
        }

        switch (reason) {
            case ERemoveReplicaReason::IncrementalHeartbeat:
            case ERemoveReplicaReason::Unapproved:
                node->RemoveChunk(chunk, cached);
                break;
            case ERemoveReplicaReason::Reset:
                // Do nothing.
                break;
            default:
                YUNREACHABLE();
        }
        chunk->RemoveLocation(node->GetId(), cached);

        if (!IsRecovery()) {
            LOG_EVENT(
                Logger,
                reason == ERemoveReplicaReason::Reset ? NLog::ELogLevel::Trace : NLog::ELogLevel::Debug,
                "Chunk replica removed (ChunkId: %s, Cached: %s, Reason: %s, Address: %s, NodeId: %d)",
                ~chunkId.ToString(),
                ~FormatBool(cached),
                ~reason.ToString(),
                ~node->GetAddress(),
                nodeId);
        }

        if (!cached && IsLeader()) {
            ChunkReplicator->ScheduleChunkRefresh(chunkId);
        }

        Profiler.Increment(RemoveChunkReplicaCounter);
    }


    void ConfirmChunk(
        TChunk* chunk,
        const std::vector<Stroka>& addresses,
        NChunkClient::NProto::TChunkInfo* chunkInfo,
        NChunkClient::NProto::TChunkMeta* chunkMeta)
    {
        YCHECK(!chunk->IsConfirmed());

        auto id = chunk->GetId();

        chunk->ChunkInfo().Swap(chunkInfo);
        chunk->ChunkMeta().Swap(chunkMeta);

        FOREACH (const auto& address, addresses) {
            auto* node = FindNodeByAddresss(address);
            if (!node) {
                LOG_DEBUG_UNLESS(IsRecovery(), "Tried to confirm chunk %s at an unknown node %s",
                    ~id.ToString(),
                    ~address);
                continue;
            }

            if (node->GetState() != ENodeState::Online) {
                LOG_DEBUG_UNLESS(IsRecovery(), "Tried to confirm chunk %s at node %s with invalid state %s",
                    ~id.ToString(),
                    ~address,
                    ~FormatEnum(node->GetState()));
                continue;
            }

            if (!node->HasChunk(chunk, false)) {
                AddChunkReplica(
                    node,
                    chunk,
                    false,
                    EAddReplicaReason::Confirmation);
                node->MarkChunkUnapproved(chunk);
            }
        }

        if (IsLeader()) {
            ChunkReplicator->ScheduleChunkRefresh(id);
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Chunk confirmed (ChunkId: %s)", ~id.ToString());
    }


    void AddJob(TDataNode* node, const TJobStartInfo& jobInfo)
    {
        auto* mutationContext = Bootstrap
            ->GetMetaStateFacade()
            ->GetManager()
            ->GetMutationContext();

        auto chunkId = TChunkId::FromProto(jobInfo.chunk_id());
        auto jobId = TJobId::FromProto(jobInfo.job_id());
        auto targetAddresses = FromProto<Stroka>(jobInfo.target_addresses());
        auto jobType = EJobType(jobInfo.type());

        auto* job = new TJob(
            jobType,
            jobId,
            chunkId,
            node->GetAddress(),
            targetAddresses,
            mutationContext->GetTimestamp());
        JobMap.Insert(jobId, job);

        auto* jobList = GetOrCreateJobList(chunkId);
        jobList->AddJob(job);

        node->AddJob(job);

        RegisterReplicationSinks(job);

        LOG_INFO_UNLESS(IsRecovery(), "Job added (JobId: %s, Address: %s, NodeId: %d, JobType: %s, ChunkId: %s)",
            ~jobId.ToString(),
            ~node->GetAddress(),
            node->GetId(),
            ~jobType.ToString(),
            ~chunkId.ToString());
    }

    void RemoveJob(
        TJob* job,
        bool removeFromNode,
        bool removeFromJobList)
    {
        auto jobId = job->GetId();

        if (removeFromJobList) {
            auto* jobList = GetJobList(job->GetChunkId());
            jobList->RemoveJob(job);
            DropJobListIfEmpty(jobList);
        }

        if (removeFromNode) {
            auto* node = FindNodeByAddresss(job->GetAddress());
            if (node) {
                node->RemoveJob(job);
            }
        }

        if (IsLeader()) {
            ChunkReplicator->ScheduleChunkRefresh(job->GetChunkId());
        }

        UnregisterReplicationSinks(job);

        JobMap.Remove(jobId);

        LOG_INFO_UNLESS(IsRecovery(), "Job removed (JobId: %s)", ~jobId.ToString());
    }


    void ProcessAddedChunk(
        TDataNode* node,
        const TChunkAddInfo& chunkAddInfo,
        bool incremental)
    {
        auto nodeId = node->GetId();
        auto chunkId = TChunkId::FromProto(chunkAddInfo.chunk_id());
        bool cached = chunkAddInfo.cached();

        auto* chunk = FindChunk(chunkId);
        if (!chunk) {
            // Nodes may still contain cached replicas of chunks that no longer exist.
            // Here we just silently ignore this case.
            if (cached) {
                return;
            }

            LOG_DEBUG_UNLESS(IsRecovery(), "Unknown chunk added, removal scheduled (Address: %s, NodeId: %d, ChunkId: %s, Cached: %s)",
                ~node->GetAddress(),
                nodeId,
                ~chunkId.ToString(),
                ~FormatBool(cached));

            if (IsLeader()) {
                ChunkReplicator->ScheduleChunkRemoval(node, chunkId);
            }

            return;
        }

        if (!cached && node->HasUnapprovedChunk(chunk)) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Chunk approved (Address: %s, NodeId: %d, ChunkId: %s)",
                ~node->GetAddress(),
                nodeId,
                ~chunkId.ToString());

            node->ApproveChunk(chunk);
            return;
        }

        // Use the size reported by the node, but check it for consistency first.
        if (!chunk->ValidateChunkInfo(chunkAddInfo.chunk_info())) {
            auto error = TError("Mismatched chunk info reported by node (ChunkId: %s, Cached: %s, ExpectedInfo: {%s}, ReceivedInfo: {%s}, Address: %s, HolderId: %d)",
                ~chunkId.ToString(),
                ~FormatBool(cached),
                ~chunk->ChunkInfo().DebugString(),
                ~chunkAddInfo.chunk_info().DebugString(),
                ~node->GetAddress(),
                node->GetId());
            LOG_ERROR(error);
            // TODO(babenko): return error to node
            return;
        }
        chunk->ChunkInfo() = chunkAddInfo.chunk_info();

        AddChunkReplica(
            node,
            chunk,
            cached,
            incremental ? EAddReplicaReason::IncrementalHeartbeat : EAddReplicaReason::FullHeartbeat);
    }

    void ProcessRemovedChunk(
        TDataNode* node,
        const TChunkRemoveInfo& chunkInfo)
    {
        auto nodeId = node->GetId();
        auto chunkId = TChunkId::FromProto(chunkInfo.chunk_id());
        bool cached = chunkInfo.cached();

        auto* chunk = FindChunk(chunkId);
        if (!chunk) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Unknown chunk replica removed (ChunkId: %s, Cached: %s, Address: %s, NodeId: %d)",
                 ~chunkId.ToString(),
                 ~FormatBool(cached),
                 ~node->GetAddress(),
                 nodeId);
            return;
        }

        RemoveChunkReplica(
            node,
            chunk,
            cached,
            ERemoveReplicaReason::IncrementalHeartbeat);
    }


    TJobList* GetOrCreateJobList(const TChunkId& id)
    {
        auto* jobList = FindJobList(id);
        if (!jobList) {
            jobList = new TJobList(id);
            JobListMap.Insert(id, jobList);
        }
        return jobList;
    }

    void DropJobListIfEmpty(const TJobList* jobList)
    {
        if (jobList->Jobs().empty()) {
            JobListMap.Remove(jobList->GetChunkId());
        }
    }


    void RegisterReplicationSinks(TJob* job)
    {
        switch (job->GetType()) {
            case EJobType::Replicate: {
                FOREACH (const auto& address, job->TargetAddresses()) {
                    auto& sink = GetOrCreateReplicationSink(address);
                    YCHECK(sink.Jobs().insert(job).second);
                }
                break;
            }

            case EJobType::Remove:
                break;

            default:
                YUNREACHABLE();
        }
    }

    void UnregisterReplicationSinks(TJob* job)
    {
        switch (job->GetType()) {
            case EJobType::Replicate: {
                FOREACH (const auto& address, job->TargetAddresses()) {
                    auto& sink = GetOrCreateReplicationSink(address);
                    YCHECK(sink.Jobs().erase(job) == 1);
                    DropReplicationSinkIfEmpty(sink);
                }
                break;
            }

            case EJobType::Remove:
                break;

            default:
                YUNREACHABLE();
        }
    }

    TReplicationSink& GetOrCreateReplicationSink(const Stroka& address)
    {
        auto it = ReplicationSinkMap.find(address);
        if (it != ReplicationSinkMap.end()) {
            return it->second;
        }

        auto pair = ReplicationSinkMap.insert(MakePair(address, TReplicationSink(address)));
        YCHECK(pair.second);

        return pair.first->second;
    }

    void DropReplicationSinkIfEmpty(const TReplicationSink& sink)
    {
        if (sink.Jobs().empty()) {
            // NB: Do not try to inline this variable! erase() will destroy the object
            // and will access the key afterwards.
            auto address = sink.GetAddress();
            YCHECK(ReplicationSinkMap.erase(address) == 1);
        }
    }

    static void GetOwningNodes(
        TChunkTreeRef chunkRef,
        yhash_set<TChunkTreeRef>& visitedRefs,
        yhash_set<ICypressNode*>* owningNodes)
    {
        if (!visitedRefs.insert(chunkRef).second) {
            return;
        }
        switch (chunkRef.GetType()) {
            case EObjectType::Chunk: {
                FOREACH (auto* parent, chunkRef.AsChunk()->Parents()) {
                    GetOwningNodes(parent, visitedRefs, owningNodes);
                }
                break;
            }
            case EObjectType::ChunkList: {
                auto* chunkList = chunkRef.AsChunkList();
                owningNodes->insert(chunkList->OwningNodes().begin(), chunkList->OwningNodes().end());
                FOREACH (auto* parent, chunkList->Parents()) {
                    GetOwningNodes(parent, visitedRefs, owningNodes);
                }
                break;
            }
            default:
                YUNREACHABLE();
        }
    }

    void GetOwningNodes(TChunkTreeRef chunkRef, IYsonConsumer* consumer)
    {
        auto cypressManager = Bootstrap->GetCypressManager();

        yhash_set<ICypressNode*> owningNodes;
        yhash_set<TChunkTreeRef> visitedRefs;
        GetOwningNodes(chunkRef, visitedRefs, &owningNodes);

        std::vector<TYPath> paths;
        FOREACH (auto* node, owningNodes) {
            auto proxy = cypressManager->GetVersionedNodeProxy(node->GetId());
            auto path = proxy->GetPath();
            paths.push_back(path);
        }

        std::sort(paths.begin(), paths.end());
        paths.erase(std::unique(paths.begin(), paths.end()), paths.end());

        BuildYsonFluently(consumer)
            .DoListFor(paths, [] (TFluentList fluent, const TYPath& path) {
                fluent.Item().Scalar(path);
            });
    }

};

DEFINE_METAMAP_ACCESSORS(TChunkManager::TImpl, Chunk, TChunk, TChunkId, ChunkMap)
DEFINE_METAMAP_ACCESSORS(TChunkManager::TImpl, ChunkList, TChunkList, TChunkListId, ChunkListMap)
DEFINE_METAMAP_ACCESSORS(TChunkManager::TImpl, Node, TDataNode, TNodeId, NodeMap)
DEFINE_METAMAP_ACCESSORS(TChunkManager::TImpl, JobList, TJobList, TChunkId, JobListMap)
DEFINE_METAMAP_ACCESSORS(TChunkManager::TImpl, Job, TJob, TJobId, JobMap)

DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, yhash_set<TChunkId>, LostChunkIds, *ChunkReplicator);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, yhash_set<TChunkId>, OverreplicatedChunkIds, *ChunkReplicator);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, yhash_set<TChunkId>, UnderreplicatedChunkIds, *ChunkReplicator);

///////////////////////////////////////////////////////////////////////////////

class TChunkManager::TChunkProxy
    : public NObjectServer::TUnversionedObjectProxyBase<TChunk>
{
public:
    TChunkProxy(TIntrusivePtr<TImpl> owner, const TChunkId& id)
        : TBase(owner->Bootstrap, id, &owner->ChunkMap)
        , Owner(owner)
    {
        Logger = ChunkServerLogger;
    }

    virtual bool IsWriteRequest(NRpc::IServiceContextPtr context) const override
    {
        DECLARE_YPATH_SERVICE_WRITE_METHOD(Confirm);
        return TBase::IsWriteRequest(context);
    }

private:
    typedef TUnversionedObjectProxyBase<TChunk> TBase;

    TIntrusivePtr<TImpl> Owner;

    virtual void GetSystemAttributes(std::vector<TAttributeInfo>* attributes) override
    {
        const auto* chunk = GetTypedImpl();
        auto miscExt = FindProtoExtension<TMiscExt>(chunk->ChunkMeta().extensions());

        YCHECK(!chunk->IsConfirmed() || miscExt);

        attributes->push_back("confirmed");
        attributes->push_back("cached_locations");
        attributes->push_back("stored_locations");
        attributes->push_back("replication_factor");
        attributes->push_back("movable");
        attributes->push_back("master_meta_size");
        attributes->push_back(TAttributeInfo("owning_nodes", true, true));
        attributes->push_back(TAttributeInfo("size", chunk->IsConfirmed()));
        attributes->push_back(TAttributeInfo("chunk_type", chunk->IsConfirmed()));
        attributes->push_back(TAttributeInfo("meta_size", chunk->IsConfirmed() && miscExt->has_meta_size()));
        attributes->push_back(TAttributeInfo("compressed_data_size", chunk->IsConfirmed() && miscExt->has_compressed_data_size()));
        attributes->push_back(TAttributeInfo("uncompressed_data_size", chunk->IsConfirmed() && miscExt->has_uncompressed_data_size()));
        attributes->push_back(TAttributeInfo("data_weight", chunk->IsConfirmed() && miscExt->has_data_weight()));
        attributes->push_back(TAttributeInfo("codec_id", chunk->IsConfirmed() && miscExt->has_codec_id()));
        attributes->push_back(TAttributeInfo("row_count", chunk->IsConfirmed() && miscExt->has_row_count()));
        attributes->push_back(TAttributeInfo("value_count", chunk->IsConfirmed() && miscExt->has_value_count()));
        attributes->push_back(TAttributeInfo("sorted", chunk->IsConfirmed() && miscExt->has_sorted()));
        TBase::GetSystemAttributes(attributes);
    }

    virtual bool GetSystemAttribute(const Stroka& name, IYsonConsumer* consumer) override
    {
        const auto* chunk = GetTypedImpl();

        if (name == "confirmed") {
            BuildYsonFluently(consumer)
                .Scalar(FormatBool(chunk->IsConfirmed()));
            return true;
        }

        if (name == "cached_locations") {
            if (~chunk->CachedLocations()) {
                BuildYsonFluently(consumer)
                    .DoListFor(*chunk->CachedLocations(), [=] (TFluentList fluent, TNodeId nodeId) {
                        const auto& node = Owner->GetNode(nodeId);
                        fluent.Item().Scalar(node->GetAddress());
                    });
            } else {
                BuildYsonFluently(consumer)
                    .BeginList()
                    .EndList();
            }
            return true;
        }

        if (name == "stored_locations") {
            BuildYsonFluently(consumer)
                .DoListFor(chunk->StoredLocations(), [=] (TFluentList fluent, TNodeId nodeId) {
                    const auto& node = Owner->GetNode(nodeId);
                    fluent.Item().Scalar(node->GetAddress());
                });
            return true;
        }

        if (name == "replication_factor") {
            BuildYsonFluently(consumer)
                .Scalar(chunk->GetReplicationFactor());
            return true;
        }

        if (name == "movable") {
            BuildYsonFluently(consumer)
                .Scalar(chunk->GetMovable());
            return true;
        }

        if (name == "master_meta_size") {
            BuildYsonFluently(consumer)
                .Scalar(chunk->ChunkMeta().ByteSize());
            return true;
        }

        if (name == "owning_nodes") {
            Owner->GetOwningNodes(TChunkTreeRef(const_cast<TChunk*>(chunk)), consumer);
            return true;
        }

        if (chunk->IsConfirmed()) {
            auto miscExt = GetProtoExtension<TMiscExt>(chunk->ChunkMeta().extensions());

            if (name == "size") {
                BuildYsonFluently(consumer)
                    .Scalar(chunk->ChunkInfo().size());
                return true;
            }

            if (name == "chunk_type") {
                auto type = EChunkType(chunk->ChunkMeta().type());
                BuildYsonFluently(consumer)
                    .Scalar(CamelCaseToUnderscoreCase(type.ToString()));
                return true;
            }

            if (name == "meta_size") {
                BuildYsonFluently(consumer)
                    .Scalar(miscExt.meta_size());
                return true;
            }

            if (name == "compressed_data_size") {
                BuildYsonFluently(consumer)
                    .Scalar(miscExt.compressed_data_size());
                return true;
            }

            if (name == "uncompressed_data_size") {
                BuildYsonFluently(consumer)
                    .Scalar(miscExt.uncompressed_data_size());
                return true;
            }

            if (name == "data_weight") {
                BuildYsonFluently(consumer)
                    .Scalar(miscExt.data_weight());
                return true;
            }

            if (name == "codec_id") {
                BuildYsonFluently(consumer)
                    .Scalar(CamelCaseToUnderscoreCase(ECodecId(miscExt.codec_id()).ToString()));
                return true;
            }

            if (name == "row_count") {
                BuildYsonFluently(consumer)
                    .Scalar(miscExt.row_count());
                return true;
            }

            if (name == "value_count") {
                BuildYsonFluently(consumer)
                    .Scalar(miscExt.value_count());
                return true;
            }

            if (name == "sorted") {
                BuildYsonFluently(consumer)
                    .Scalar(FormatBool(miscExt.sorted()));
                return true;
            }
        }

        return TBase::GetSystemAttribute(name, consumer);
    }

    virtual void DoInvoke(IServiceContextPtr context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Locate);
        DISPATCH_YPATH_SERVICE_METHOD(Fetch);
        DISPATCH_YPATH_SERVICE_METHOD(Confirm);
        TBase::DoInvoke(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, Locate)
    {
        UNUSED(request);

        const auto* chunk = GetTypedImpl();
        Owner->FillNodeAddresses(response->mutable_node_addresses(), chunk);

        context->SetResponseInfo("NodeAddresses: [%s]",
            ~JoinToString(response->node_addresses()));

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTableClient::NProto, Fetch)
    {
        UNUSED(request);

        const auto* chunk = GetTypedImpl();

        if (chunk->ChunkMeta().type() != EChunkType::Table) {
            THROW_ERROR_EXCEPTION("Unable to execute Fetch verb for non-table chunk");
        }

        auto* inputChunk = response->add_chunks();
        *inputChunk->mutable_slice()->mutable_chunk_id() = chunk->GetId().ToProto();
        inputChunk->mutable_slice()->mutable_start_limit();
        inputChunk->mutable_slice()->mutable_end_limit();
        *inputChunk->mutable_channel() = NTableClient::TChannel::CreateUniversal().ToProto();
        inputChunk->mutable_extensions()->CopyFrom(chunk->ChunkMeta().extensions());

        auto miscExt = GetProtoExtension<TMiscExt>(chunk->ChunkMeta().extensions());
        inputChunk->set_row_count(miscExt.row_count());
        inputChunk->set_uncompressed_data_size(miscExt.uncompressed_data_size());

        if (request->fetch_node_addresses()) {
            Owner->FillNodeAddresses(inputChunk->mutable_node_addresses(), chunk);
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, Confirm)
    {
        UNUSED(response);

        auto addresses = FromProto<Stroka>(request->node_addresses());
        YCHECK(!addresses.empty());

        context->SetRequestInfo("Size: %" PRId64 ", Addresses: [%s]",
            request->chunk_info().size(),
            ~JoinToString(addresses));

        auto* chunk = GetTypedImpl();

        // Skip chunks that are already confirmed.
        if (chunk->IsConfirmed()) {
            context->SetResponseInfo("Chunk is already confirmed");
            context->Reply();
            return;
        }

        // Use the size reported by the client, but check it for consistency first.
        if (!chunk->ValidateChunkInfo(request->chunk_info())) {
            LOG_FATAL("Invalid chunk info reported by client (ChunkId: %s, ExpectedInfo: {%s}, ReceivedInfo: {%s})",
                ~Id.ToString(),
                ~chunk->ChunkInfo().DebugString(),
                ~request->chunk_info().DebugString());
        }

        Owner->ConfirmChunk(
            chunk,
            addresses,
            request->mutable_chunk_info(),
            request->mutable_chunk_meta());

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

TChunkManager::TChunkTypeHandler::TChunkTypeHandler(TImpl* owner)
    : TObjectTypeHandlerBase(owner->Bootstrap, &owner->ChunkMap)
    , Owner(owner)
{ }

IObjectProxyPtr TChunkManager::TChunkTypeHandler::GetProxy(
    const TObjectId& id,
    TTransaction* transaction)
{
    UNUSED(transaction);
    return New<TChunkProxy>(Owner, id);
}

TObjectId TChunkManager::TChunkTypeHandler::Create(
    TTransaction* transaction,
    TReqCreateObject* request,
    TRspCreateObject* response)
{
    UNUSED(transaction);

    const auto* requestExt = &request->GetExtension(TReqCreateChunkExt::create_chunk);
    auto* responseExt = response->MutableExtension(TRspCreateChunkExt::create_chunk);

    auto* chunk = Owner->CreateChunk();
    chunk->SetReplicationFactor(requestExt->replication_factor());
    chunk->SetMovable(requestExt->movable());

    if (Owner->IsLeader()) {
        auto preferredHostName =
            requestExt->has_preferred_host_name()
            ? TNullable<Stroka>(requestExt->preferred_host_name())
            : Null;

        auto nodes = Owner->AllocateUploadTargets(
            requestExt->upload_replication_factor(),
            preferredHostName);

        FOREACH (auto* node, nodes) {
            responseExt->add_node_addresses(node->GetAddress());
        }

        LOG_DEBUG_UNLESS(Owner->IsRecovery(), "Allocated nodes [%s] for chunk %s (PreferredHostName: %s, ReplicationFactor: %d, UploadReplicationFactor: %d, Movable: %s)",
            ~JoinToString(responseExt->node_addresses()),
            ~chunk->GetId().ToString(),
            ~ToString(preferredHostName),
            requestExt->replication_factor(),
            requestExt->upload_replication_factor(),
            ~FormatBool(requestExt->movable()));
    }

    return chunk->GetId();
}

void TChunkManager::TChunkTypeHandler::DoDestroy(TChunk* chunk)
{
    Owner->DestroyChunk(chunk);
}

///////////////////////////////////////////////////////////////////////////////

class TChunkManager::TChunkListProxy
    : public NObjectServer::TUnversionedObjectProxyBase<TChunkList>
{
public:
    TChunkListProxy(TIntrusivePtr<TImpl> owner, const TChunkListId& id)
        : TBase(owner->Bootstrap, id, &owner->ChunkListMap)
        , Owner(owner)
    {
        Logger = ChunkServerLogger;
    }

    virtual bool IsWriteRequest(NRpc::IServiceContextPtr context) const override
    {
        DECLARE_YPATH_SERVICE_WRITE_METHOD(Attach);
        return TBase::IsWriteRequest(context);
    }

private:
    typedef TUnversionedObjectProxyBase<TChunkList> TBase;

    TIntrusivePtr<TImpl> Owner;

    virtual void GetSystemAttributes(std::vector<TAttributeInfo>* attributes) override
    {
        attributes->push_back("children_ids");
        attributes->push_back("parent_ids");
        attributes->push_back("row_count");
        attributes->push_back("uncompressed_data_size");
        attributes->push_back("compressed_size");
        attributes->push_back("chunk_count");
        attributes->push_back("rank");
        attributes->push_back("rigid");
        attributes->push_back(TAttributeInfo("tree", true, true));
        attributes->push_back(TAttributeInfo("owning_nodes", true, true));
        TBase::GetSystemAttributes(attributes);
    }

    void TraverseTree(TChunkTreeRef ref, IYsonConsumer* consumer)
    {
        switch (ref.GetType()) {
            case EObjectType::Chunk:
                consumer->OnStringScalar(ref.GetId().ToString());
                break;
            case EObjectType::ChunkList: {
                const auto* chunkList = ref.AsChunkList();
                consumer->OnBeginAttributes();
                consumer->OnKeyedItem("id");
                consumer->OnStringScalar(chunkList->GetId().ToString());
                consumer->OnKeyedItem("rank");
                consumer->OnIntegerScalar(chunkList->Statistics().Rank);
                consumer->OnEndAttributes();

                consumer->OnBeginList();
                FOREACH (auto childRef, chunkList->Children()) {
                    consumer->OnListItem();
                    TraverseTree(childRef, consumer);
                }
                consumer->OnEndList();
                break;
            }
            default:
                YUNREACHABLE();
        }
    }

    virtual bool GetSystemAttribute(const Stroka& name, IYsonConsumer* consumer) override
    {
        const auto* chunkList = GetTypedImpl();

        if (name == "children_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(chunkList->Children(), [=] (TFluentList fluent, TChunkTreeRef chunkRef) {
                        fluent.Item().Scalar(chunkRef.GetId());
                });
            return true;
        }

        if (name == "parent_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(chunkList->Parents(), [=] (TFluentList fluent, TChunkList* chunkList) {
                    fluent.Item().Scalar(chunkList->GetId());
                });
            return true;
        }

        const auto& statistics = chunkList->Statistics();

        if (name == "row_count") {
            BuildYsonFluently(consumer)
                .Scalar(statistics.RowCount);
            return true;
        }

        if (name == "uncompressed_data_size") {
            BuildYsonFluently(consumer)
                .Scalar(statistics.UncompressedSize);
            return true;
        }

        if (name == "compressed_size") {
            BuildYsonFluently(consumer)
                .Scalar(statistics.CompressedSize);
            return true;
        }

        if (name == "chunk_count") {
            BuildYsonFluently(consumer)
                .Scalar(statistics.ChunkCount);
            return true;
        }

        if (name == "rank") {
            BuildYsonFluently(consumer)
                .Scalar(statistics.Rank);
            return true;
        }

        if (name == "rigid") {
            BuildYsonFluently(consumer)
                .Scalar(chunkList->GetRigid());
            return true;
        }

        if (name == "tree") {
            TraverseTree(const_cast<TChunkList*>(chunkList), consumer);
            return true;
        }

        if (name == "owning_nodes") {
            Owner->GetOwningNodes(const_cast<TChunkList*>(chunkList), consumer);
            return true;
        }

        return TBase::GetSystemAttribute(name, consumer);
    }

    virtual void DoInvoke(NRpc::IServiceContextPtr context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Attach);
        TBase::DoInvoke(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, Attach)
    {
        UNUSED(response);

        auto childrenIds = FromProto<TChunkTreeId>(request->children_ids());

        context->SetRequestInfo("Children: [%s]", ~JoinToString(childrenIds));

        auto objectManager = Bootstrap->GetObjectManager();
        std::vector<TChunkTreeRef> childrenRefs;
        FOREACH (const auto& childId, childrenIds) {
            if (!objectManager->ObjectExists(childId)) {
                THROW_ERROR_EXCEPTION("Chunk tree %s does not exist", ~childId.ToString());
            }
            auto chunkRef = Owner->GetChunkTree(childId);
            childrenRefs.push_back(chunkRef);
        }

        auto* chunkList = GetTypedImpl();
        Owner->AttachToChunkList(chunkList, childrenRefs);

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

TChunkManager::TChunkListTypeHandler::TChunkListTypeHandler(TImpl* owner)
    : TObjectTypeHandlerBase(owner->Bootstrap, &owner->ChunkListMap)
    , Owner(owner)
{ }

IObjectProxyPtr TChunkManager::TChunkListTypeHandler::GetProxy(
    const TObjectId& id,
    TTransaction* transaction)
{
    UNUSED(transaction);
    return New<TChunkListProxy>(Owner, id);
}

TObjectId TChunkManager::TChunkListTypeHandler::Create(
    TTransaction* transaction,
    TReqCreateObject* request,
    TRspCreateObject* response)
{
    UNUSED(transaction);
    UNUSED(request);
    UNUSED(response);

    auto* chunkList = Owner->CreateChunkList();
    return chunkList->GetId();
}

void TChunkManager::TChunkListTypeHandler::DoDestroy(TChunkList* chunkList)
{
    Owner->DestroyChunkList(chunkList);
}

////////////////////////////////////////////////////////////////////////////////

TChunkManager::TChunkManager(
    TChunkManagerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl(New<TImpl>(config, bootstrap))
{ }

TChunkManager::~TChunkManager()
{ }

TDataNode* TChunkManager::FindNodeByAddress(const Stroka& address)
{
    return Impl->FindNodeByAddresss(address);
}

TDataNode* TChunkManager::FindNodeByHostName(const Stroka& hostName)
{
    return Impl->FindNodeByHostName(hostName);
}

const TReplicationSink* TChunkManager::FindReplicationSink(const Stroka& address)
{
    return Impl->FindReplicationSink(address);
}

std::vector<TDataNode*> TChunkManager::AllocateUploadTargets(
    int nodeCount,
    TNullable<Stroka> preferredHostName)
{
    return Impl->AllocateUploadTargets(nodeCount, preferredHostName);
}

TMutationPtr TChunkManager::CreateRegisterNodeMutation(
    const TMetaReqRegisterNode& request)
{
    return Impl->CreateRegisterNodeMutation(request);
}

TMutationPtr TChunkManager::CreateUnregisterNodeMutation(
    const TMetaReqUnregisterNode& request)
{
    return Impl->CreateUnregisterNodeMutation(request);
}

TMutationPtr TChunkManager::CreateFullHeartbeatMutation(
    TCtxFullHeartbeatPtr context)
{
    return Impl->CreateFullHeartbeatMutation(context);
}

TMutationPtr TChunkManager::CreateIncrementalHeartbeatMutation(
    const TMetaReqIncrementalHeartbeat& request)
{
    return Impl->CreateIncrementalHeartbeatMutation(request);
}

TMutationPtr TChunkManager::CreateUpdateJobsMutation(
    const TMetaReqUpdateJobs& request)
{
    return Impl->CreateUpdateJobsMutation(request);
}

TChunk* TChunkManager::CreateChunk()
{
    return Impl->CreateChunk();
}

TChunkList* TChunkManager::CreateChunkList()
{
    return Impl->CreateChunkList();
}

void TChunkManager::AttachToChunkList(
    TChunkList* chunkList,
    const TChunkTreeRef* childrenBegin,
    const TChunkTreeRef* childrenEnd)
{
    Impl->AttachToChunkList(chunkList, childrenBegin, childrenEnd);
}

void TChunkManager::AttachToChunkList(
    TChunkList* chunkList,
    const std::vector<TChunkTreeRef>& children)
{
    Impl->AttachToChunkList(chunkList, children);
}

void TChunkManager::AttachToChunkList(
    TChunkList* chunkList,
    const TChunkTreeRef childRef)
{
    Impl->AttachToChunkList(chunkList, childRef);
}

void TChunkManager::ClearChunkList(TChunkList* chunkList)
{
    Impl->ClearChunkList(chunkList);
}

void TChunkManager::SetChunkTreeParent(TChunkList* parent, TChunkTreeRef childRef)
{
    return Impl->SetChunkTreeParent(parent, childRef);
}

void TChunkManager::ResetChunkTreeParent(TChunkList* parent, TChunkTreeRef childRef)
{
    return Impl->ResetChunkTreeParent(parent, childRef);
}

void TChunkManager::ScheduleJobs(
    TDataNode* node,
    const std::vector<TJobInfo>& runningJobs,
    std::vector<TJobStartInfo>* jobsToStart,
    std::vector<TJobStopInfo>* jobsToStop)
{
    Impl->ScheduleJobs(
        node,
        runningJobs,
        jobsToStart,
        jobsToStop);
}

bool TChunkManager::IsReplicatorEnabled()
{
    return Impl->IsReplicatorEnabled();
}

void TChunkManager::FillNodeAddresses(
    ::google::protobuf::RepeatedPtrField< TProtoStringType>* addresses,
    const TChunk* chunk)
{
    Impl->FillNodeAddresses(addresses, chunk);
}

TTotalNodeStatistics TChunkManager::GetTotalNodeStatistics()
{
    return Impl->GetTotalNodeStatistics();
}

bool TChunkManager::IsNodeConfirmed(const TDataNode* node)
{
    return Impl->IsNodeConfirmed(node);
}

i32 TChunkManager::GetChunkReplicaCount()
{
    return Impl->GetChunkReplicaCount();
}

DELEGATE_METAMAP_ACCESSORS(TChunkManager, Chunk, TChunk, TChunkId, *Impl)
DELEGATE_METAMAP_ACCESSORS(TChunkManager, ChunkList, TChunkList, TChunkListId, *Impl)
DELEGATE_METAMAP_ACCESSORS(TChunkManager, Node, TDataNode, TNodeId, *Impl)
DELEGATE_METAMAP_ACCESSORS(TChunkManager, JobList, TJobList, TChunkId, *Impl)
DELEGATE_METAMAP_ACCESSORS(TChunkManager, Job, TJob, TJobId, *Impl)

DELEGATE_SIGNAL(TChunkManager, void(const TDataNode*), NodeRegistered, *Impl);
DELEGATE_SIGNAL(TChunkManager, void(const TDataNode*), NodeUnregistered, *Impl);

DELEGATE_BYREF_RO_PROPERTY(TChunkManager, yhash_set<TChunkId>, LostChunkIds, *Impl);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, yhash_set<TChunkId>, OverreplicatedChunkIds, *Impl);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, yhash_set<TChunkId>, UnderreplicatedChunkIds, *Impl);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
