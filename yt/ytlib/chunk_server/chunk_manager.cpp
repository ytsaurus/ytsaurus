#include "stdafx.h"
#include "chunk_manager.h"
#include "chunk_manager.pb.h"
#include "chunk_placement.h"
#include "chunk_replication.h"
#include "holder_lease_tracker.h"
#include "holder_statistics.h"
#include "chunk_ypath.pb.h"
#include "chunk_list_ypath.pb.h"
#include "file_chunk_meta.pb.h"
#include "table_chunk_meta.pb.h"

#include <ytlib/cell_master/load_context.h>
#include <ytlib/misc/foreach.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/misc/guid.h>
#include <ytlib/misc/id_generator.h>
#include <ytlib/misc/string.h>
#include <ytlib/cell_master/bootstrap.h>
#include <ytlib/transaction_server/transaction_manager.h>
#include <ytlib/transaction_server/transaction.h>
#include <ytlib/meta_state/meta_state_manager.h>
#include <ytlib/meta_state/composite_meta_state.h>
#include <ytlib/meta_state/map.h>
#include <ytlib/object_server/type_handler_detail.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/logging/log.h>
#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NChunkServer {

using namespace NProto;
using namespace NMetaState;
using namespace NTransactionServer;
using namespace NObjectServer;
using namespace NYTree;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ChunkServer");
static NProfiling::TProfiler Profiler("chunk_server");

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TChunkTypeHandler
    : public TObjectTypeHandlerBase<TChunk>
{
public:
    TChunkTypeHandler(TImpl* owner);

    virtual EObjectType GetType()
    {
        return EObjectType::Chunk;
    }

    virtual TObjectId CreateFromManifest(
        const TTransactionId& transactionId,
        IMapNode* manifest);

    virtual IObjectProxy::TPtr GetProxy(const TVersionedObjectId& id);

private:
    TImpl* Owner;

    virtual void OnObjectDestroyed(TChunk& chunk);

};

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TChunkListTypeHandler
    : public TObjectTypeHandlerBase<TChunkList>
{
public:
    TChunkListTypeHandler(TImpl* owner);

    virtual EObjectType GetType()
    {
        return EObjectType::ChunkList;
    }

    virtual TObjectId CreateFromManifest(
        const TTransactionId& transactionId,
        IMapNode* manifest);

    virtual IObjectProxy::TPtr GetProxy(const TVersionedObjectId& id);

private:
    TImpl* Owner;

    virtual void OnObjectDestroyed(TChunkList& chunkList);
};

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TImpl
    : public NMetaState::TMetaStatePart
{
public:
    typedef TIntrusivePtr<TImpl> TPtr;

    TImpl(
        TConfig* config,
        TBootstrap* bootstrap)
        : TMetaStatePart(
            ~bootstrap->GetMetaStateManager(),
            ~bootstrap->GetMetaState())
        , Config(config)
        , Bootstrap(bootstrap)
		, ChunkReplicaCount(0)
    {
        YASSERT(config);
        YASSERT(bootstrap);

        RegisterMethod(this, &TImpl::FullHeartbeat);
        RegisterMethod(this, &TImpl::IncrementalHeartbeat);
        RegisterMethod(this, &TImpl::UpdateJobs);
        RegisterMethod(this, &TImpl::RegisterHolder);
        RegisterMethod(this, &TImpl::UnregisterHolder);
        RegisterMethod(this, &TImpl::CreateChunks);

        TLoadContext context(bootstrap);

        auto metaState = bootstrap->GetMetaState();
        metaState->RegisterLoader(
            "ChunkManager.Keys.1",
            FromMethod(&TChunkManager::TImpl::LoadKeys, MakeStrong(this)));
        metaState->RegisterLoader(
            "ChunkManager.Values.1",
            FromMethod(&TChunkManager::TImpl::LoadValues, MakeStrong(this), context));
        metaState->RegisterSaver(
            "ChunkManager.Keys.1",
            FromMethod(&TChunkManager::TImpl::SaveKeys, MakeStrong(this)),
            ESavePhase::Keys);
        metaState->RegisterSaver(
            "ChunkManager.Values.1",
            FromMethod(&TChunkManager::TImpl::SaveValues, MakeStrong(this)),
            ESavePhase::Values);

        metaState->RegisterPart(this);

        auto objectManager = bootstrap->GetObjectManager();
        objectManager->RegisterHandler(~New<TChunkTypeHandler>(this));
        objectManager->RegisterHandler(~New<TChunkListTypeHandler>(this));
    }

    TMetaChange<THolderId>::TPtr InitiateRegisterHolder(
        const TMsgRegisterHolder& message)
    {
        return CreateMetaChange(
            ~MetaStateManager,
            message,
            &TThis::RegisterHolder,
            this);
    }

    TMetaChange<TVoid>::TPtr  InitiateUnregisterHolder(
        const TMsgUnregisterHolder& message)
    {
        return CreateMetaChange(
            ~MetaStateManager,
            message,
            &TThis::UnregisterHolder,
            this);
    }

    TMetaChange<TVoid>::TPtr InitiateFullHeartbeat(
        const TMsgFullHeartbeat& message)
    {
        return CreateMetaChange(
            ~MetaStateManager,
            message,
            &TThis::FullHeartbeat,
            this);
    }

    TMetaChange<TVoid>::TPtr InitiateIncrementalHeartbeat(
        const TMsgIncrementalHeartbeat& message)
    {
        return CreateMetaChange(
            ~MetaStateManager,
            message,
            &TThis::IncrementalHeartbeat,
            this);
    }

    TMetaChange<TVoid>::TPtr InitiateUpdateJobs(
        const TMsgUpdateJobs& message)
    {
        return CreateMetaChange(
            ~MetaStateManager,
            message,
            &TThis::UpdateJobs,
            this);
    }

    TMetaChange< yvector<TChunkId> >::TPtr InitiateCreateChunks(
        const TMsgCreateChunks& message)
    {
        return CreateMetaChange(
            ~MetaStateManager,
            message,
            &TThis::CreateChunks,
            this);
    }


    DECLARE_METAMAP_ACCESSORS(Chunk, TChunk, TChunkId);
    DECLARE_METAMAP_ACCESSORS(ChunkList, TChunkList, TChunkListId);
    DECLARE_METAMAP_ACCESSORS(Holder, THolder, THolderId);
    DECLARE_METAMAP_ACCESSORS(JobList, TJobList, TChunkId);
    DECLARE_METAMAP_ACCESSORS(Job, TJob, TJobId);

    DEFINE_SIGNAL(void(const THolder&), HolderRegistered);
    DEFINE_SIGNAL(void(const THolder&), HolderUnregistered);


    const THolder* FindHolder(const Stroka& address) const
    {
        auto it = HolderAddressMap.find(address);
        return it == HolderAddressMap.end() ? NULL : FindHolder(it->second);
    }

    THolder* FindHolder(const Stroka& address)
    {
        auto it = HolderAddressMap.find(address);
        return it == HolderAddressMap.end() ? NULL : FindHolder(it->second);
    }

    const TReplicationSink* FindReplicationSink(const Stroka& address)
    {
        auto it = ReplicationSinkMap.find(address);
        return it == ReplicationSinkMap.end() ? NULL : &it->second;
    }

    yvector<THolderId> AllocateUploadTargets(int replicaCount)
    {
        auto holderIds = ChunkPlacement->GetUploadTargets(replicaCount);
        FOREACH (auto holderId, holderIds) {
            const auto& holder = GetHolder(holderId);
            ChunkPlacement->OnSessionHinted(holder);
        }
        return holderIds;
    }


    TChunk& CreateChunk()
    {
        auto id = Bootstrap->GetObjectManager()->GenerateId(EObjectType::Chunk);
        auto* chunk = new TChunk(id);
        ChunkMap.Insert(id, chunk);
        return *chunk;
    }

    TChunkList& CreateChunkList()
    {
        auto id = Bootstrap->GetObjectManager()->GenerateId(EObjectType::ChunkList);
        auto* chunkList = new TChunkList(id);
        ChunkListMap.Insert(id, chunkList);
        return *chunkList;
    }


    void AttachToChunkList(TChunkList& chunkList, const yvector<TChunkTreeId>& childrenIds)
    {
        auto objectManager = Bootstrap->GetObjectManager();
        FOREACH (const auto& childId, childrenIds) {
            chunkList.ChildrenIds().push_back(childId);
            SetChunkTreeParent(chunkList, childId);
            objectManager->RefObject(childId);
        }
    }

    void DetachFromChunkList(TChunkList& chunkList, const yvector<TChunkTreeId>& childrenIds)
    {
        auto objectManager = Bootstrap->GetObjectManager();
        yhash_set<TChunkTreeId> childrenIdsSet(childrenIds.begin(), childrenIds.end());
        auto it = chunkList.ChildrenIds().begin();
        while (it != chunkList.ChildrenIds().end()) {
            auto jt = it;
            ++jt;
            const auto& childId = *it;
            if (childrenIdsSet.find(childId) != childrenIdsSet.end()) {
                chunkList.ChildrenIds().erase(it);
                ResetChunkTreeParent(chunkList, childId, true);
                objectManager->UnrefObject(childId);
            }
            it = jt;
        }
    }


    void RunJobControl(
        const THolder& holder,
        const yvector<TJobInfo>& runningJobs,
        yvector<TJobStartInfo>* jobsToStart,
        yvector<TJobStopInfo>* jobsToStop)
    {
        ChunkReplication->RunJobControl(
            holder,
            runningJobs,
            jobsToStart,
            jobsToStop);
    }

    const yhash_set<TChunkId>& LostChunkIds() const;
    const yhash_set<TChunkId>& OverreplicatedChunkIds() const;
    const yhash_set<TChunkId>& UnderreplicatedChunkIds() const;

    void FillHolderAddresses(
        ::google::protobuf::RepeatedPtrField<TProtoStringType>* addresses,
        const TChunk& chunk)
    {
        FOREACH (auto holderId, chunk.StoredLocations()) {
            const THolder& holder = GetHolder(holderId);
            addresses->Add()->assign(holder.GetAddress());
        }

        if (~chunk.CachedLocations()) {
            FOREACH (auto holderId, *chunk.CachedLocations()) {
                const THolder& holder = GetHolder(holderId);
                addresses->Add()->assign(holder.GetAddress());
            }
        }
    }

    TTotalHolderStatistics GetTotalHolderStatistics() const
    {
        TTotalHolderStatistics result;
        auto keys = HolderMap.GetKeys();
        FOREACH (const auto& key, keys) {
            const auto& holder = HolderMap.Get(key);
            const auto& statistics = holder.Statistics();
            result.AvailbaleSpace += statistics.available_space();
            result.UsedSpace += statistics.used_space();
            result.ChunkCount += statistics.chunk_count();
            result.SessionCount += statistics.session_count();
            result.AliveHolderCount++;
        }
        return result;
    }

private:
    typedef TImpl TThis;
    friend class TChunkTypeHandler;
    friend class TChunkProxy;
    friend class TChunkListTypeHandler;
    friend class TChunkListProxy;

    TConfig::TPtr Config;
    TBootstrap* Bootstrap;
    
    TChunkPlacement::TPtr ChunkPlacement;
    TChunkReplication::TPtr ChunkReplication;
    THolderLeaseTracker::TPtr HolderLeaseTracking;
    
    TIdGenerator<THolderId> HolderIdGenerator;

    TMetaStateMap<TChunkId, TChunk> ChunkMap;
    TMetaStateMap<TChunkListId, TChunkList> ChunkListMap;
	i32 ChunkReplicaCount;

    TMetaStateMap<THolderId, THolder> HolderMap;
    yhash_map<Stroka, THolderId> HolderAddressMap;

    TMetaStateMap<TChunkId, TJobList> JobListMap;
    TMetaStateMap<TJobId, TJob> JobMap;

    yhash_map<Stroka, TReplicationSink> ReplicationSinkMap;


    yvector<TChunkId> CreateChunks(const TMsgCreateChunks& message)
    {
        auto objectManager = Bootstrap->GetObjectManager();
        auto transactionManager = Bootstrap->GetTransactionManager();
        auto transactionId = TTransactionId::FromProto(message.transaction_id());
        int chunkCount = message.chunk_count();

        yvector<TChunkId> chunkIds;
        chunkIds.reserve(chunkCount);
        for (int index = 0; index < chunkCount; ++index) {
            auto chunkId = objectManager->GenerateId(EObjectType::Chunk);
            auto* chunk = new TChunk(chunkId);
            ChunkMap.Insert(chunkId, chunk);

            // The newly created chunk is referenced from the transaction.
            auto& transaction = transactionManager->GetTransaction(transactionId);
            YVERIFY(transaction.CreatedObjectIds().insert(chunkId).second);
            objectManager->RefObject(chunkId);

            chunkIds.push_back(chunkId);
        }

        LOG_INFO_IF(!IsRecovery(), "Chunks allocated (ChunkIds: [%s], TransactionId: %s)",
            ~JoinToString(chunkIds),
            ~transactionId.ToString());

        return chunkIds;
    }


    TChunkStatistics GetChunkStatistics(const TChunk& chunk)
    {
        TChunkStatistics result;

        YASSERT(chunk.GetSize() != TChunk::UnknownSize);
        result.CompressedSize = chunk.GetSize();

        auto attributes = chunk.DeserializeAttributes();
        switch (attributes.type()) {
            case EChunkType::File: {
                const auto& fileAttributes = attributes.GetExtension(NFileClient::NProto::TFileChunkAttributes::file_attributes);
                result.UncompressedSize = fileAttributes.uncompressed_size();
                break;
            }

            case EChunkType::Table: {
                const auto& tableAttributes = attributes.GetExtension(NTableClient::NProto::TTableChunkAttributes::table_attributes);
                result.RowCount = tableAttributes.row_count();
                result.UncompressedSize = tableAttributes.uncompressed_size();
                break;
            }

            default:
                YUNREACHABLE();
        }

        return result;
    }

    void UpdateStatistics(TChunkList& chunkList, const TChunkTreeId& childId, bool negate)
    {
        // Compute delta.
        TChunkStatistics delta;
        switch (TypeFromId(childId)) {
            case EObjectType::Chunk:
                delta = GetChunkStatistics(GetChunk(childId));
                break;
            case EObjectType::ChunkList:
                delta = GetChunkList(childId).Statistics();
                break;
            default:
                YUNREACHABLE();
        }

        // Negate delta if necessary.
        if (negate) {
            delta.Negate();
        }

        // Apply delta to the parent chunk list.
        chunkList.Statistics().Accumulate(delta);

        // Run a DFS-like traversal upwards and apply delta.
        // TODO(babenko): this only works correctly if upward paths are unique.
        std::vector<TChunkListId> dfsStack(chunkList.ParentIds().begin(), chunkList.ParentIds().end());
        while (!dfsStack.empty()) {
            auto currentChunkListId = dfsStack.back();
            dfsStack.pop_back();
            auto& currentChunkList = GetChunkList(currentChunkListId);
            currentChunkList.Statistics().Accumulate(delta);
            dfsStack.insert(dfsStack.end(), currentChunkList.ParentIds().begin(), currentChunkList.ParentIds().end());
        }
    }


    void SetChunkTreeParent(TChunkList& parent, const TChunkTreeId& childId)
    {
        if (TypeFromId(childId) == EObjectType::ChunkList) {
            auto& childChunkList = GetChunkList(childId);
            YVERIFY(childChunkList.ParentIds().insert(parent.GetId()).second);
        }
        UpdateStatistics(parent, childId, false);
    }

    void ResetChunkTreeParent(TChunkList& parent, const TChunkTreeId& childId, bool updateStatistics)
    {
        if (TypeFromId(childId) == EObjectType::ChunkList) {
            auto& childChunkList = GetChunkList(childId);
            YVERIFY(childChunkList.ParentIds().erase(parent.GetId()) == 1);
        }
        if (updateStatistics) {
            UpdateStatistics(parent, childId, true);
        }
    }


    void OnChunkDestroyed(TChunk& chunk)
    {
        auto chunkId = chunk.GetId();

        // Unregister chunk replicas from all known locations.
        FOREACH (auto holderId, chunk.StoredLocations()) {
            RemoveChunkAtLocation(holderId, chunk, false);
        }
        if (~chunk.CachedLocations()) {
            FOREACH (auto holderId, *chunk.CachedLocations()) {
                RemoveChunkAtLocation(holderId, chunk, true);
            }
        }

        // Remove all associated jobs.
        auto* jobList = FindJobList(chunk.GetId());
        if (jobList) {
            FOREACH (const auto& jobId, jobList->JobIds()) {
                auto& job = GetJob(jobId);
                auto* holder = FindHolder(job.GetRunnerAddress());
                if (holder) {
                    RemoveJob(*holder, job, false);
                }
            }
        }
    }

    void OnChunkListDestroyed(TChunkList& chunkList)
    {
        auto objectManager = Bootstrap->GetObjectManager();
        // Drop references to children.
        FOREACH (const auto& childId, chunkList.ChildrenIds()) {
            ResetChunkTreeParent(chunkList, childId, false);
            objectManager->UnrefObject(childId);
        }
    }


    THolderId RegisterHolder(const TMsgRegisterHolder& message)
    {
        Stroka address = message.address();
        auto incarnationId = TIncarnationId::FromProto(message.incarnation_id());
        const auto& statistics = message.statistics();
    
        THolderId holderId = HolderIdGenerator.Next();
    
        auto* existingHolder = FindHolder(address);
        if (existingHolder) {
            LOG_INFO_IF(!IsRecovery(), "Holder kicked out due to address conflict (Address: %s, HolderId: %d)",
                ~address,
                existingHolder->GetId());
            DoUnregisterHolder(*existingHolder);
        }

        LOG_INFO_IF(!IsRecovery(), "Holder registered (Address: %s, HolderId: %d, IncarnationId: %s, %s)",
            ~address,
            holderId,
            ~incarnationId.ToString(),
            ~ToString(statistics));

        auto* newHolder = new THolder(
            holderId,
            address,
            incarnationId,
            EHolderState::Registered,
            statistics);

        HolderMap.Insert(holderId, newHolder);
        HolderAddressMap.insert(MakePair(address, holderId)).second;

        if (IsLeader()) {
            StartHolderTracking(*newHolder);
        }

        return holderId;
    }

    TVoid UnregisterHolder(const TMsgUnregisterHolder& message)
    { 
        auto holderId = message.holder_id();

        auto& holder = GetHolder(holderId);
        DoUnregisterHolder(holder);

        return TVoid();
    }


    TVoid FullHeartbeat(const TMsgFullHeartbeat& message)
    {
        PROFILE_TIMING ("full_heartbeat") {
            auto holderId = message.holder_id();
            const auto& statistics = message.statistics();

            auto& holder = GetHolder(holderId);

            LOG_DEBUG_IF(!IsRecovery(), "Full heartbeat received (Address: %s, HolderId: %d, State: %s, %s, Chunks: %d)",
                ~holder.GetAddress(),
                holderId,
                ~holder.GetState().ToString(),
                ~ToString(statistics),
                static_cast<int>(message.chunks_size()));

            YASSERT(holder.GetState() == EHolderState::Registered);
            holder.SetState(EHolderState::Online);
            holder.Statistics() = statistics;

            if (IsLeader()) {
                HolderLeaseTracking->OnHolderOnline(holder);
                ChunkPlacement->OnHolderUpdated(holder);
            }

            std::vector<TChunkId> storedChunkIds(holder.StoredChunkIds().begin(), holder.StoredChunkIds().end());
            FOREACH (const auto& chunkId, storedChunkIds) {
                RemoveChunkReplica(holder, GetChunk(chunkId), false, ERemoveReplicaReason::Reset);
            }
            holder.StoredChunkIds().clear();

            std::vector<TChunkId> cachedChunkIds(holder.CachedChunkIds().begin(), holder.CachedChunkIds().end());
            FOREACH (const auto& chunkId, cachedChunkIds) {
                RemoveChunkReplica(holder, GetChunk(chunkId), true, ERemoveReplicaReason::Reset);
            }
            holder.CachedChunkIds().clear();

            FOREACH (const auto& chunkInfo, message.chunks()) {
                ProcessAddedChunk(holder, chunkInfo, false);
            }
        }
        return TVoid();
    }

    TVoid IncrementalHeartbeat(const TMsgIncrementalHeartbeat& message)
    {
        PROFILE_TIMING ("incremental_heartbeat") {
            auto holderId = message.holder_id();
            const auto& statistics = message.statistics();

            auto& holder = GetHolder(holderId);

            LOG_DEBUG_IF(!IsRecovery(), "Incremental heartbeat received (Address: %s, HolderId: %d, State: %s, %s, ChunksAdded: %d, ChunksRemoved: %d)",
                ~holder.GetAddress(),
                holderId,
                ~holder.GetState().ToString(),
                ~ToString(statistics),
                static_cast<int>(message.added_chunks_size()),
                static_cast<int>(message.removed_chunks_size()));

            YASSERT(holder.GetState() == EHolderState::Online);
            holder.Statistics() = statistics;

            if (IsLeader()) {
                HolderLeaseTracking->OnHolderHeartbeat(holder);
                ChunkPlacement->OnHolderUpdated(holder);
            }

            FOREACH (const auto& chunkInfo, message.added_chunks()) {
                ProcessAddedChunk(holder, chunkInfo, true);
            }

            FOREACH (const auto& chunkInfo, message.removed_chunks()) {
                ProcessRemovedChunk(holder, chunkInfo);
            }

            std::vector<TChunkId> unapprovedChunkIds(holder.UnapprovedChunkIds().begin(), holder.UnapprovedChunkIds().end());
            FOREACH (const auto& chunkId, unapprovedChunkIds) {
                RemoveChunkReplica(holder, GetChunk(chunkId), false, ERemoveReplicaReason::Unapproved);
            }
            holder.UnapprovedChunkIds().clear();
        }
        return TVoid();
    }

    TVoid UpdateJobs(const TMsgUpdateJobs& message)
    {
        PROFILE_TIMING ("update_jobs") {
            auto holderId = message.holder_id();
            auto& holder = GetHolder(holderId);

            FOREACH (const auto& startInfo, message.started_jobs()) {
                AddJob(holder, startInfo);
            }

            FOREACH (const auto& stopInfo, message.stopped_jobs()) {
                auto jobId = TJobId::FromProto(stopInfo.job_id());
                const auto* job = FindJob(jobId);
                if (job) {
                    RemoveJob(holder, *job, false);
                }
            }

            LOG_DEBUG_IF(!IsRecovery(), "Holder jobs updated (Address: %s, HolderId: %d, JobsStarted: %d, JobsStopped: %d)",
                ~holder.GetAddress(),
                holderId,
                static_cast<int>(message.started_jobs_size()),
                static_cast<int>(message.stopped_jobs_size()));
        }
        return TVoid();
    }


    void SaveKeys(TOutputStream* output)
    {
        ChunkMap.SaveKeys(output);
        ChunkListMap.SaveKeys(output);
        HolderMap.SaveKeys(output);
        JobMap.SaveKeys(output);
        JobListMap.SaveKeys(output);
    }

    void SaveValues(TOutputStream* output)
    {
        ::Save(output, HolderIdGenerator);

        ChunkMap.SaveValues(output);
        ChunkListMap.SaveValues(output);
        HolderMap.SaveValues(output);
        JobMap.SaveValues(output);
        JobListMap.SaveValues(output);
    }

    void LoadKeys(TInputStream* input)
    {
        ChunkMap.LoadKeys(input);
        ChunkListMap.LoadKeys(input);
        HolderMap.LoadKeys(input);
        JobMap.LoadKeys(input);
        JobListMap.LoadKeys(input);
    }

    void LoadValues(TInputStream* input, TLoadContext context)
    {
        ::Load(input, HolderIdGenerator);

        ChunkMap.LoadValues(input, context);
        ChunkListMap.LoadValues(input, context);
        HolderMap.LoadValues(input, context);
        JobMap.LoadValues(input, context);
        JobListMap.LoadValues(input, context);

		// Compute chunk replica count.
		ChunkReplicaCount = 0;
		FOREACH (const auto& pair, HolderMap) {
			const auto& holder = *pair.second;
			ChunkReplicaCount += holder.StoredChunkIds().size();
			ChunkReplicaCount += holder.CachedChunkIds().size();
		}

		// Reconstruct HolderAddressMap.
        HolderAddressMap.clear();
        FOREACH (const auto& pair, HolderMap) {
            const auto* holder = pair.second;
            YVERIFY(HolderAddressMap.insert(MakePair(holder->GetAddress(), holder->GetId())).second);
        }

        // Reconstruct ReplicationSinkMap.
        ReplicationSinkMap.clear();
        FOREACH (const auto& pair, JobMap) {
            RegisterReplicationSinks(*pair.second);
        }
    }

    virtual void Clear()
    {
        HolderIdGenerator.Reset();
        ChunkMap.Clear();
		ChunkReplicaCount = 0;
        ChunkListMap.Clear();
        HolderMap.Clear();
        JobMap.Clear();
        JobListMap.Clear();

        HolderAddressMap.clear();
        ReplicationSinkMap.clear();
    }


    virtual void OnLeaderRecoveryComplete()
    {
        ChunkPlacement = New<TChunkPlacement>(~Config, Bootstrap);

        ChunkReplication = New<TChunkReplication>(~Config, Bootstrap, ~ChunkPlacement);

        HolderLeaseTracking = New<THolderLeaseTracker>(~Config, Bootstrap);

        FOREACH (const auto& pair, HolderMap) {
            StartHolderTracking(*pair.second);
        }
    }

    virtual void OnStopLeading()
    {
        ChunkPlacement.Reset();
        ChunkReplication.Reset();
        HolderLeaseTracking.Reset();
    }


    void StartHolderTracking(const THolder& holder)
    {
        HolderLeaseTracking->OnHolderRegistered(holder);
        ChunkPlacement->OnHolderRegistered(holder);
        ChunkReplication->OnHolderRegistered(holder);

        HolderRegistered_.Fire(holder);
    }

    void StopHolderTracking(const THolder& holder)
    {
        HolderLeaseTracking->OnHolderUnregistered(holder);
        ChunkPlacement->OnHolderUnregistered(holder);
        ChunkReplication->OnHolderUnregistered(holder);

        HolderUnregistered_.Fire(holder);
    }


    void DoUnregisterHolder(THolder& holder)
    { 
        PROFILE_TIMING("holder_unregistration") {
            auto holderId = holder.GetId();

            LOG_INFO_IF(!IsRecovery(), "Holder unregistered (Address: %s, HolderId: %d)",
                ~holder.GetAddress(),
                holderId);

            if (IsLeader()) {
                StopHolderTracking(holder);
            }

            FOREACH (const auto& chunkId, holder.StoredChunkIds()) {
                auto& chunk = GetChunk(chunkId);
                RemoveChunkReplica(holder, chunk, false, ERemoveReplicaReason::Reset);
            }

            FOREACH (const auto& chunkId, holder.CachedChunkIds()) {
                auto& chunk = GetChunk(chunkId);
                RemoveChunkReplica(holder, chunk, true, ERemoveReplicaReason::Reset);
            }

            FOREACH (const auto& jobId, holder.JobIds()) {
                auto& job = GetJob(jobId);
                // Pass true to suppress removal of job ids from holder.
                RemoveJob(holder, job, true);
            }

            YVERIFY(HolderAddressMap.erase(holder.GetAddress()) == 1);
            HolderMap.Remove(holderId);
        }
    }


    DECLARE_ENUM(EAddReplicaReason,
        (IncrementalHeartbeat)
        (FullHeartbeat)
        (Confirmation)
    );

    void AddChunkReplica(THolder& holder, TChunk& chunk, bool cached, EAddReplicaReason reason)
    {
        auto chunkId = chunk.GetId();
        auto holderId = holder.GetId();

        if (holder.HasChunk(chunkId, cached)) {
            LOG_WARNING_IF(!IsRecovery(), "Chunk replica is already added (ChunkId: %s, Cached: %s, Reason: %s, Address: %s, HolderId: %d)",
                ~chunkId.ToString(),
                ~FormatBool(cached),
                ~reason.ToString(),
                ~holder.GetAddress(),
                holderId);
            return;
        }

        holder.AddChunk(chunkId, cached);
        chunk.AddLocation(holderId, cached);

        if (!IsRecovery()) {
            LOG_EVENT(
                Logger,
                reason == EAddReplicaReason::FullHeartbeat ? NLog::ELogLevel::Trace : NLog::ELogLevel::Debug,
                "Chunk replica added (ChunkId: %s, Cached: %s, Address: %s, HolderId: %d)",
                ~chunkId.ToString(),
                ~FormatBool(cached),
                ~holder.GetAddress(),
                holderId);
        }

        if (!cached && IsLeader()) {
            ChunkReplication->ScheduleChunkRefresh(chunk.GetId());
        }
    }

    void RemoveChunkAtLocation(THolderId holderId, TChunk& chunk, bool cached)
    {
        auto chunkId = chunk.GetId();
        auto& holder = GetHolder(holderId);
        holder.RemoveChunk(chunkId, cached);

        if (!cached && IsLeader()) {
            ChunkReplication->ScheduleChunkRemoval(holder, chunkId);
        }
    }

    DECLARE_ENUM(ERemoveReplicaReason,
        (IncrementalHeartbeat)
        (Unapproved)
        (Reset)
    );

    void RemoveChunkReplica(THolder& holder, TChunk& chunk, bool cached, ERemoveReplicaReason reason)
    {
        auto chunkId = chunk.GetId();
        auto holderId = holder.GetId();

        if (reason == ERemoveReplicaReason::IncrementalHeartbeat && !holder.HasChunk(chunkId, cached)) {
            LOG_WARNING_IF(!IsRecovery(), "Chunk replica is already removed (ChunkId: %s, Cached: %s, Reason: %s, Address: %s, HolderId: %d)",
                ~chunkId.ToString(),
                ~FormatBool(cached),
                ~reason.ToString(),
                ~holder.GetAddress(),
                holderId);
            return;
        }

        switch (reason) {
            case ERemoveReplicaReason::IncrementalHeartbeat:
            case ERemoveReplicaReason::Unapproved:
                holder.RemoveChunk(chunk.GetId(), cached);
                break;
            case ERemoveReplicaReason::Reset:
                // Do nothing.
                break;
            default:
                YUNREACHABLE();
        }
        chunk.RemoveLocation(holder.GetId(), cached);

        if (!IsRecovery()) {
            LOG_EVENT(
                Logger,
                reason == ERemoveReplicaReason::Reset ? NLog::ELogLevel::Trace : NLog::ELogLevel::Debug,
                "Chunk replica removed (ChunkId: %s, Cached: %s, Reason: %s, Address: %s, HolderId: %d)",
                ~chunkId.ToString(),
                ~FormatBool(cached),
                ~reason.ToString(),
                ~holder.GetAddress(),
                holderId);
        }

        if (!cached && IsLeader()) {
            ChunkReplication->ScheduleChunkRefresh(chunk.GetId());
        }
    }


    void AddJob(THolder& holder, const TJobStartInfo& jobInfo)
    {
        auto chunkId = TChunkId::FromProto(jobInfo.chunk_id());
        auto jobId = TJobId::FromProto(jobInfo.job_id());
        auto targetAddresses = FromProto<Stroka>(jobInfo.target_addresses());
        auto jobType = EJobType(jobInfo.type());
        auto startTime = TInstant(jobInfo.start_time());

        auto* job = new TJob(
            jobType,
            jobId,
            chunkId,
            holder.GetAddress(),
            targetAddresses,
            startTime);
        JobMap.Insert(jobId, job);

        auto& list = GetOrCreateJobList(chunkId);
        list.AddJob(jobId);

        holder.AddJob(jobId);

        RegisterReplicationSinks(*job);

        LOG_INFO_IF(!IsRecovery(), "Job added (JobId: %s, Address: %s, HolderId: %d, JobType: %s, ChunkId: %s)",
            ~jobId.ToString(),
            ~holder.GetAddress(),
            holder.GetId(),
            ~jobType.ToString(),
            ~chunkId.ToString());
    }

    void RemoveJob(THolder& holder, const TJob& job, bool holderDied)
    {
        auto jobId = job.GetJobId();

        auto& list = GetJobList(job.GetChunkId());
        list.RemoveJob(jobId);
        DropJobListIfEmpty(list);

        if (!holderDied) {
            holder.RemoveJob(jobId);
        }

        if (IsLeader()) {
            ChunkReplication->ScheduleChunkRefresh(job.GetChunkId());
        }

        UnregisterReplicationSinks(job);

        JobMap.Remove(job.GetJobId());

        LOG_INFO_IF(!IsRecovery(), "Job removed (JobId: %s, Address: %s, HolderId: %d)",
            ~jobId.ToString(),
            ~holder.GetAddress(),
            holder.GetId());
    }


    void ProcessAddedChunk(
        THolder& holder,
        const TChunkAddInfo& chunkInfo,
        bool incremental)
    {
        auto holderId = holder.GetId();
        auto chunkId = TChunkId::FromProto(chunkInfo.chunk_id());
        i64 size = chunkInfo.size();
        bool cached = chunkInfo.cached();

        if (!cached && holder.HasUnapprovedChunk(chunkId)) {
            LOG_DEBUG_IF(!IsRecovery(), "Chunk approved (Address: %s, HolderId: %d, ChunkId: %s)",
                ~holder.GetAddress(),
                holderId,
                ~chunkId.ToString());

            holder.ApproveChunk(chunkId);
            return;
        }

        auto* chunk = FindChunk(chunkId);
        if (!chunk) {
            // Holders may still contain cached replicas of chunks that no longer exist.
            // Here we just silently ignore this case.
            if (cached) {
                return;
            }

            LOG_DEBUG_IF(!IsRecovery(), "Unknown chunk added at holder, removal scheduled (Address: %s, HolderId: %d, ChunkId: %s, Cached: %s, Size: %" PRId64 ")",
                ~holder.GetAddress(),
                holderId,
                ~chunkId.ToString(),
                ~FormatBool(cached),
                size);

            if (IsLeader()) {
                ChunkReplication->ScheduleChunkRemoval(holder, chunkId);
            }

            return;
        }

        // Use the size reported by the holder, but check it for consistency first.
        if (chunk->GetSize() != TChunk::UnknownSize && chunk->GetSize() != size) {
            LOG_FATAL("Mismatched chunk size reported by holder (ChunkId: %s, Cached: %s, KnownSize: %" PRId64 ", NewSize: %" PRId64 ", Address: %s, HolderId: %d)",
                ~chunkId.ToString(),
                ~ToString(cached),
                chunk->GetSize(),
                size,
                ~holder.GetAddress(),
                holder.GetId());
        }
        chunk->SetSize(size);

        AddChunkReplica(
            holder,
            *chunk,
            cached,
            incremental ? EAddReplicaReason::IncrementalHeartbeat : EAddReplicaReason::FullHeartbeat);
    }

    void ProcessRemovedChunk(
        THolder& holder,
        const TChunkRemoveInfo& chunkInfo)
    {
        auto holderId = holder.GetId();
        auto chunkId = TChunkId::FromProto(chunkInfo.chunk_id());
        bool cached = chunkInfo.cached();

        auto* chunk = FindChunk(chunkId);
        if (!chunk) {
            LOG_DEBUG_IF(!IsRecovery(), "Unknown chunk replica removed (ChunkId: %s, Cached: %s, Address: %s, HolderId: %d)",
                 ~chunkId.ToString(),
                 ~FormatBool(cached),
                 ~holder.GetAddress(),
                 holderId);
            return;
        }

        RemoveChunkReplica(
            holder,
            *chunk,
            cached,
            ERemoveReplicaReason::IncrementalHeartbeat);
    }


    TJobList& GetOrCreateJobList(const TChunkId& id)
    {
        auto* list = FindJobList(id);
        if (list)
            return *list;

        JobListMap.Insert(id, new TJobList(id));
        return GetJobList(id);
    }

    void DropJobListIfEmpty(const TJobList& list)
    {
        if (list.JobIds().empty()) {
            JobListMap.Remove(list.GetChunkId());
        }
    }


    void RegisterReplicationSinks(const TJob& job)
    {
        switch (job.GetType()) {
            case EJobType::Replicate: {
                FOREACH (const auto& address, job.TargetAddresses()) {
                    auto& sink = GetOrCreateReplicationSink(address);
                    YASSERT(sink.JobIds().insert(job.GetJobId()).second);
                }
                break;
            }

            case EJobType::Remove:
                break;

            default:
                YUNREACHABLE();
        }
    }

    void UnregisterReplicationSinks(const TJob& job)
    {
        switch (job.GetType()) {
            case EJobType::Replicate: {
                FOREACH (const auto& address, job.TargetAddresses()) {
                    auto& sink = GetOrCreateReplicationSink(address);
                    YASSERT(sink.JobIds().erase(job.GetJobId()) == 1);
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
        if (it != ReplicationSinkMap.end())
            return it->second;

        auto pair = ReplicationSinkMap.insert(MakePair(address, TReplicationSink(address)));
        YASSERT(pair.second);
        return pair.first->second;
    }

    void DropReplicationSinkIfEmpty(const TReplicationSink& sink)
    {
        if (sink.JobIds().empty()) {
            // NB: Do not try to inline this variable! erase() will destroy the object
            // and will access the key afterwards.
            auto address = sink.GetAddress();
            YVERIFY(ReplicationSinkMap.erase(address) == 1);
        }
    }

};

DEFINE_METAMAP_ACCESSORS(TChunkManager::TImpl, Chunk, TChunk, TChunkId, ChunkMap)
DEFINE_METAMAP_ACCESSORS(TChunkManager::TImpl, ChunkList, TChunkList, TChunkListId, ChunkListMap)
DEFINE_METAMAP_ACCESSORS(TChunkManager::TImpl, Holder, THolder, THolderId, HolderMap)
DEFINE_METAMAP_ACCESSORS(TChunkManager::TImpl, JobList, TJobList, TChunkId, JobListMap)
DEFINE_METAMAP_ACCESSORS(TChunkManager::TImpl, Job, TJob, TJobId, JobMap)

DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, yhash_set<TChunkId>, LostChunkIds, *ChunkReplication);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, yhash_set<TChunkId>, OverreplicatedChunkIds, *ChunkReplication);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, yhash_set<TChunkId>, UnderreplicatedChunkIds, *ChunkReplication);

///////////////////////////////////////////////////////////////////////////////

class TChunkManager::TChunkProxy
    : public NObjectServer::TUnversionedObjectProxyBase<TChunk>
{
public:
    TChunkProxy(TImpl* owner, const TChunkId& id)
        : TBase(owner->Bootstrap, id, &owner->ChunkMap)
        , TYPathServiceBase(ChunkServerLogger.GetCategory())
        , Owner(owner)
    { }

    virtual bool IsWriteRequest(NRpc::IServiceContext* context) const
    {
        DECLARE_YPATH_SERVICE_WRITE_METHOD(Confirm);
        return TBase::IsWriteRequest(context);
    }

private:
    typedef TUnversionedObjectProxyBase<TChunk> TBase;

    TIntrusivePtr<TImpl> Owner;

    virtual void GetSystemAttributes(std::vector<TAttributeInfo>* attributes)
    {
        const auto& chunk = GetTypedImpl();
        attributes->push_back("confirmed");
        attributes->push_back("cached_locations");
        attributes->push_back("stored_locations");
        attributes->push_back(TAttributeInfo("size", chunk.IsConfirmed()));
        attributes->push_back(TAttributeInfo("chunk_type", chunk.IsConfirmed()));
        TBase::GetSystemAttributes(attributes);
    }

    virtual bool GetSystemAttribute(const Stroka& name, NYTree::IYsonConsumer* consumer)
    {
        const auto& chunk = GetTypedImpl();

        if (name == "confirmed") {
            BuildYsonFluently(consumer)
                .Scalar(FormatBool(chunk.IsConfirmed()));
            return true;
        }

        if (name == "cached_locations") {
            if (~chunk.CachedLocations()) {
                BuildYsonFluently(consumer)
                    .DoListFor(*chunk.CachedLocations(), [=] (TFluentList fluent, THolderId holderId)
                        {
                            const auto& holder = Owner->GetHolder(holderId);
                            fluent.Item().Scalar(holder.GetAddress());
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
                .DoListFor(chunk.StoredLocations(), [=] (TFluentList fluent, THolderId holderId)
                    {
                        const auto& holder = Owner->GetHolder(holderId);
                        fluent.Item().Scalar(holder.GetAddress());
                    });
            return true;
        }

        if (chunk.IsConfirmed()) {
            if (name == "size") {
                BuildYsonFluently(consumer)
                    .Scalar(chunk.GetSize());
                return true;
            }

            if (name == "chunk_type") {
                auto attributes = chunk.DeserializeAttributes();
                auto type = EChunkType(attributes.type());
                BuildYsonFluently(consumer)
                    .Scalar(CamelCaseToUnderscoreCase(type.ToString()));
                return true;
            }
        }

        return TBase::GetSystemAttribute(name, consumer);
    }

    virtual void DoInvoke(NRpc::IServiceContext* context)
    {
        DISPATCH_YPATH_SERVICE_METHOD(Fetch);
        DISPATCH_YPATH_SERVICE_METHOD(Confirm);
        TBase::DoInvoke(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Fetch)
    {
        UNUSED(request);

        const auto& chunk = GetTypedImpl();
        Owner->FillHolderAddresses(response->mutable_holder_addresses(), chunk);

        context->SetResponseInfo("HolderAddresses: [%s]",
            ~JoinToString(response->holder_addresses()));

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Confirm)
    {
        UNUSED(response);

        i64 size = request->size();
        auto& holderAddresses = request->holder_addresses();
        YASSERT(holderAddresses.size() != 0);

        context->SetRequestInfo("Size: %" PRId64 ", HolderAddresses: [%s]",
            size,
            ~JoinToString(holderAddresses));

        auto& chunk = GetTypedImpl();

        // Skip chunks that are already confirmed.
        if (chunk.IsConfirmed()) {
            context->SetResponseInfo("Chunk is already confirmed");
            context->Reply();
            return;
        }

        // Use the size reported by the client, but check it for consistency first.
        if (chunk.GetSize() != TChunk::UnknownSize && chunk.GetSize() != size) {
            LOG_FATAL("Mismatched chunk size reported by client (ChunkId: %s, , KnownSize: %" PRId64 ", NewSize: %" PRId64 ")",
                ~Id.ToString(),
                chunk.GetSize(),
                size);
        }
        chunk.SetSize(size);
        
        FOREACH (const auto& address, holderAddresses) {
            auto* holder = Owner->FindHolder(address);
            if (!holder) {
                LOG_WARNING("Client has confirmed a chunk at unknown holder (ChunkId: %s, HolderAddress: %s)",
                    ~Id.ToString(),
                    ~address);
                continue;
            }

            if (!holder->HasChunk(chunk.GetId(), false)) {
                Owner->AddChunkReplica(
                    *holder,
                    chunk,
                    false,
                    TImpl::EAddReplicaReason::Confirmation);
                holder->MarkChunkUnapproved(chunk.GetId());
            }
        }

        TBlob blob;
        if (!SerializeProtobuf(&request->attributes(), &blob)) {
            LOG_FATAL("Error serializing chunk attributes (ChunkId: %s)", ~Id.ToString());
        }

        chunk.SetAttributes(TSharedRef(MoveRV(blob)));

        LOG_INFO_IF(!Owner->IsRecovery(), "Chunk confirmed (ChunkId: %s)", ~Id.ToString());

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

TChunkManager::TChunkTypeHandler::TChunkTypeHandler(TImpl* owner)
    : TObjectTypeHandlerBase(owner->Bootstrap, &owner->ChunkMap)
    , Owner(owner)
{ }

IObjectProxy::TPtr TChunkManager::TChunkTypeHandler::GetProxy(const TVersionedObjectId& id)
{
    return New<TChunkProxy>(Owner, id.ObjectId);
}

TObjectId TChunkManager::TChunkTypeHandler::CreateFromManifest(
    const TTransactionId& transactionId,
    IMapNode* manifest)
{
    UNUSED(transactionId);
    UNUSED(manifest);

    auto id = Owner->CreateChunk().GetId();
    auto proxy = Bootstrap->GetObjectManager()->GetProxy(id);
    proxy->Attributes().MergeFrom(manifest);
    return id;
}

void TChunkManager::TChunkTypeHandler::OnObjectDestroyed(TChunk& chunk)
{
    Owner->OnChunkDestroyed(chunk);
}

///////////////////////////////////////////////////////////////////////////////

class TChunkManager::TChunkListProxy
    : public NObjectServer::TUnversionedObjectProxyBase<TChunkList>
{
public:
    TChunkListProxy(TImpl* owner, const TChunkListId& id)
        : TBase(owner->Bootstrap, id, &owner->ChunkListMap)
        , TYPathServiceBase(ChunkServerLogger.GetCategory())
        , Owner(owner)
    { }

    virtual bool IsWriteRequest(NRpc::IServiceContext* context) const
    {
        DECLARE_YPATH_SERVICE_WRITE_METHOD(Attach);
        DECLARE_YPATH_SERVICE_WRITE_METHOD(Detach);
        return TBase::IsWriteRequest(context);
    }

private:
    typedef TUnversionedObjectProxyBase<TChunkList> TBase;

    TIntrusivePtr<TImpl> Owner;

    virtual void GetSystemAttributes(std::vector<TAttributeInfo>* attributes)
    {
        attributes->push_back("children_ids");
        attributes->push_back("parent_ids");
        TBase::GetSystemAttributes(attributes);
    }

    virtual bool GetSystemAttribute(const Stroka& name, NYTree::IYsonConsumer* consumer)
    {
        const auto& chunkList = GetTypedImpl();

        if (name == "children_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(chunkList.ChildrenIds(), [=] (TFluentList fluent, TTransactionId id)
                    {
                        fluent.Item().Scalar(id.ToString());
                    });
            return true;
        }

        if (name == "parent_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(chunkList.ParentIds(), [=] (TFluentList fluent, TTransactionId id)
                    {
                        fluent.Item().Scalar(id.ToString());
                    });
            return true;
        }

        return TBase::GetSystemAttribute(name, consumer);
    }

    virtual void DoInvoke(NRpc::IServiceContext* context)
    {
        DISPATCH_YPATH_SERVICE_METHOD(Attach);
        DISPATCH_YPATH_SERVICE_METHOD(Detach);
        TBase::DoInvoke(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Attach)
    {
        UNUSED(response);

        auto childrenIds = FromProto<TChunkTreeId>(request->children_ids());

        context->SetRequestInfo("ChildrenIds: [%s]", ~JoinToString(childrenIds));

        auto objectManager = Bootstrap->GetObjectManager();
        FOREACH (const auto& childId, childrenIds) {
            auto type = TypeFromId(childId);
            if (type != EObjectType::Chunk && type != EObjectType::ChunkList) {
                ythrow yexception() << Sprintf("Invalid child type (ObjectId: %s)", ~childId.ToString());
            }
            if (!objectManager->ObjectExists(childId)) {
                ythrow yexception() << Sprintf("Child does not exist (ObjectId: %s)", ~childId.ToString());
            }
        }

        auto& chunkList = GetTypedImpl();
        Owner->AttachToChunkList(chunkList, childrenIds);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Detach)
    {
        UNUSED(response);

        auto childrenIds = FromProto<TChunkTreeId>(request->children_ids());
        yhash_set<TChunkTreeId> childrenIdsSet(childrenIds.begin(), childrenIds.end());

        context->SetRequestInfo("ChildrenIds: [%s]", ~JoinToString(childrenIds));

        auto& chunkList = GetTypedImpl();
        Owner->DetachFromChunkList(chunkList, childrenIds);

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

TChunkManager::TChunkListTypeHandler::TChunkListTypeHandler(TImpl* owner)
    : TObjectTypeHandlerBase(owner->Bootstrap, &owner->ChunkListMap)
    , Owner(owner)
{ }

IObjectProxy::TPtr TChunkManager::TChunkListTypeHandler::GetProxy(const TVersionedObjectId& id)
{
    return New<TChunkListProxy>(Owner, id.ObjectId);
}

TObjectId TChunkManager::TChunkListTypeHandler::CreateFromManifest(
    const TTransactionId& transactionId,
    IMapNode* manifest)
{
    UNUSED(transactionId);
    UNUSED(manifest);

    auto id = Owner->CreateChunkList().GetId();
    auto proxy = Bootstrap->GetObjectManager()->GetProxy(id);
    proxy->Attributes().MergeFrom(manifest);
    return id;
}

void TChunkManager::TChunkListTypeHandler::OnObjectDestroyed(TChunkList& chunkList)
{
    Owner->OnChunkListDestroyed(chunkList);
}

////////////////////////////////////////////////////////////////////////////////

TChunkManager::TChunkManager(
    TConfig* config,
    TBootstrap* bootstrap)
    : Impl(New<TImpl>(config, bootstrap))
{ }

const THolder* TChunkManager::FindHolder(const Stroka& address) const
{
    return Impl->FindHolder(address);
}

THolder* TChunkManager::FindHolder(const Stroka& address)
{
    return Impl->FindHolder(address);
}

const TReplicationSink* TChunkManager::FindReplicationSink(const Stroka& address)
{
    return Impl->FindReplicationSink(address);
}

yvector<THolderId> TChunkManager::AllocateUploadTargets(int replicaCount)
{
    return Impl->AllocateUploadTargets(replicaCount);
}

TMetaChange<THolderId>::TPtr TChunkManager::InitiateRegisterHolder(
    const TMsgRegisterHolder& message)
{
    return Impl->InitiateRegisterHolder(message);
}

TMetaChange<TVoid>::TPtr TChunkManager::InitiateUnregisterHolder(
    const TMsgUnregisterHolder& message)
{
    return Impl->InitiateUnregisterHolder(message);
}

TMetaChange<TVoid>::TPtr TChunkManager::InitiateFullHeartbeat(
    const TMsgFullHeartbeat& message)
{
    return Impl->InitiateFullHeartbeat(message);
}

TMetaChange<TVoid>::TPtr TChunkManager::InitiateIncrementalHeartbeat(
    const TMsgIncrementalHeartbeat& message)
{
    return Impl->InitiateIncrementalHeartbeat(message);
}

TMetaChange<TVoid>::TPtr TChunkManager::InitiateUpdateJobs(
    const TMsgUpdateJobs& message)
{
    return Impl->InitiateUpdateJobs(message);
}

TMetaChange< yvector<TChunkId> >::TPtr TChunkManager::InitiateCreateChunks(
    const TMsgCreateChunks& message)
{
    return Impl->InitiateCreateChunks(message);
}

TChunk& TChunkManager::CreateChunk()
{
    return Impl->CreateChunk();
}

TChunkList& TChunkManager::CreateChunkList()
{
    return Impl->CreateChunkList();
}

void TChunkManager::AttachToChunkList(TChunkList& chunkList, const yvector<TChunkTreeId>& childrenIds)
{
    Impl->AttachToChunkList(chunkList, childrenIds);
}

void TChunkManager::DetachFromChunkList(TChunkList& chunkList, const yvector<TChunkTreeId>& childrenIds)
{
    Impl->DetachFromChunkList(chunkList, childrenIds);
}

void TChunkManager::RunJobControl(
    const THolder& holder,
    const yvector<TJobInfo>& runningJobs,
    yvector<TJobStartInfo>* jobsToStart,
    yvector<TJobStopInfo>* jobsToStop)
{
    Impl->RunJobControl(
        holder,
        runningJobs,
        jobsToStart,
        jobsToStop);
}

void TChunkManager::FillHolderAddresses(
    ::google::protobuf::RepeatedPtrField< TProtoStringType>* addresses,
    const TChunk& chunk)
{
    Impl->FillHolderAddresses(addresses, chunk);
}

TTotalHolderStatistics TChunkManager::GetTotalHolderStatistics() const
{
    return Impl->GetTotalHolderStatistics();
}


DELEGATE_METAMAP_ACCESSORS(TChunkManager, Chunk, TChunk, TChunkId, *Impl)
DELEGATE_METAMAP_ACCESSORS(TChunkManager, ChunkList, TChunkList, TChunkListId, *Impl)
DELEGATE_METAMAP_ACCESSORS(TChunkManager, Holder, THolder, THolderId, *Impl)
DELEGATE_METAMAP_ACCESSORS(TChunkManager, JobList, TJobList, TChunkId, *Impl)
DELEGATE_METAMAP_ACCESSORS(TChunkManager, Job, TJob, TJobId, *Impl)

DELEGATE_SIGNAL(TChunkManager, void(const THolder&), HolderRegistered, *Impl);
DELEGATE_SIGNAL(TChunkManager, void(const THolder&), HolderUnregistered, *Impl);

DELEGATE_BYREF_RO_PROPERTY(TChunkManager, yhash_set<TChunkId>, LostChunkIds, *Impl);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, yhash_set<TChunkId>, OverreplicatedChunkIds, *Impl);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, yhash_set<TChunkId>, UnderreplicatedChunkIds, *Impl);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
