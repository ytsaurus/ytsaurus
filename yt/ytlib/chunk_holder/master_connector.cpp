#include "stdafx.h"
#include "master_connector.h"

#include "../rpc/client.h"
#include "../election/cell_channel.h"
#include "../misc/delayed_invoker.h"
#include "../misc/serialize.h"
#include "../misc/string.h"

#include <util/system/hostname.h>

namespace NYT {
namespace NChunkHolder {

using namespace NMetaState;
using namespace NChunkServer::NProto;
using namespace NChunkClient;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

TMasterConnector::TMasterConnector(
    TConfig* config,
    TChunkStore* chunkStore,
    TChunkCache* chunkCache,
    TSessionManager* sessionManager,
    TReplicator* replicator,
    IInvoker* serviceInvoker)
    : Config(config)
    , ChunkStore(chunkStore)
    , ChunkCache(chunkCache)
    , SessionManager(sessionManager)
    , Replicator(replicator)
    , ServiceInvoker(serviceInvoker)
    , Registered(false)
    , IncrementalHeartbeat(false)
    , HolderId(InvalidHolderId)
{
    YASSERT(chunkStore);
    YASSERT(sessionManager);
    YASSERT(replicator);
    YASSERT(serviceInvoker);

    auto channel = CreateCellChannel(~Config->Masters);
    Proxy.Reset(new TProxy(~channel));
    Proxy->SetTimeout(Config->MasterRpcTimeout);

    Address = Sprintf("%s:%d", ~HostName(), Config->RpcPort);

    // NB: No intrusive ptr for this to prevent circular dependency.
    ChunkStore->ChunkAdded().Subscribe(FromMethod(
        &TMasterConnector::OnChunkAdded,
        this));
    ChunkStore->ChunkRemoved().Subscribe(FromMethod(
        &TMasterConnector::OnChunkRemoved,
        this));
    ChunkCache->ChunkAdded().Subscribe(FromMethod(
        &TMasterConnector::OnChunkAdded,
        this));
    ChunkCache->ChunkRemoved().Subscribe(FromMethod(
        &TMasterConnector::OnChunkRemoved,
        this));

    LOG_INFO("Chunk holder address is %s, master addresses are [%s]",
        ~Address,
        ~JoinToString(Config->Masters->Addresses));

    OnHeartbeat();
}

void TMasterConnector::ScheduleHeartbeat()
{
    TDelayedInvoker::Submit(
        ~FromMethod(&TMasterConnector::OnHeartbeat, TPtr(this))
        ->Via(ServiceInvoker),
        Config->HeartbeatPeriod);
}

void TMasterConnector::OnHeartbeat()
{
    if (Registered) {
        SendHeartbeat();
    } else {
        SendRegister();
    }
}

void TMasterConnector::SendRegister()
{
    auto request = Proxy->RegisterHolder();
    
    auto statistics = ComputeStatistics();
    *request->mutable_statistics() = statistics.ToProto();

    request->set_address(Address);

    request->Invoke()->Subscribe(
        FromMethod(&TMasterConnector::OnRegisterResponse, TPtr(this))
        ->Via(ServiceInvoker));

    LOG_INFO("Register request sent (%s)",
        ~statistics.ToString());
}

THolderStatistics TMasterConnector::ComputeStatistics()
{
    THolderStatistics result;
    FOREACH(const auto& location, ChunkStore->Locations()) {
        result.AvailableSpace += location->GetAvailableSpace();
        result.UsedSpace += location->GetUsedSpace();
    }

    result.ChunkCount = ChunkStore->GetChunkCount();
    result.SessionCount = SessionManager->GetSessionCount();

    return result;
}

void TMasterConnector::OnRegisterResponse(TProxy::TRspRegisterHolder::TPtr response)
{
    ScheduleHeartbeat();

    if (!response->IsOK()) {
        OnDisconnected();

        LOG_WARNING("Error registering at master\n%s", ~response->GetError().ToString());
        return;
    }

    HolderId = response->holderid();
    Registered = true;
    IncrementalHeartbeat = false;

    LOG_INFO("Successfully registered at master (HolderId: %d)",
        HolderId);
}

void TMasterConnector::SendHeartbeat()
{
    auto request = Proxy->HolderHeartbeat();

    YASSERT(HolderId != InvalidHolderId);
    request->set_holderid(HolderId);

    auto statistics = ComputeStatistics();
    *request->mutable_statistics() = statistics.ToProto();

    if (IncrementalHeartbeat) {
        ReportedAdded = AddedSinceLastSuccess;
        ReportedRemoved = RemovedSinceLastSuccess;

        FOREACH (auto chunk, ReportedAdded) {
            *request->add_addedchunks() = GetAddInfo(~chunk);
        }

        FOREACH (auto chunk, ReportedRemoved) {
            *request->add_removedchunks() = GetRemoveInfo(~chunk);
        }
    } else {
        FOREACH (const auto& chunk, ChunkStore->GetChunks()) {
            *request->add_addedchunks() = GetAddInfo(~chunk);
        }

        FOREACH (const auto& chunk, ChunkCache->GetChunks()) {
            *request->add_addedchunks() = GetAddInfo(~chunk);
        }
    }

    FOREACH (const auto& job, Replicator->GetAllJobs()) {
        auto* info = request->add_jobs();
        info->set_jobid(job->GetJobId().ToProto());
        info->set_state(job->GetState());
    }

    request->Invoke()->Subscribe(
        FromMethod(&TMasterConnector::OnHeartbeatResponse, TPtr(this))
        ->Via(ServiceInvoker));

    LOG_DEBUG("Heartbeat sent (%s, AddedChunks: %d, RemovedChunks: %d, Jobs: %d)",
        ~statistics.ToString(),
        static_cast<int>(request->addedchunks_size()),
        static_cast<int>(request->removedchunks_size()),
        static_cast<int>(request->jobs_size()));
}

TReqHolderHeartbeat::TChunkAddInfo TMasterConnector::GetAddInfo(const TChunk* chunk)
{
    TReqHolderHeartbeat::TChunkAddInfo info;
    info.set_chunkid(chunk->GetId().ToProto());
    info.set_cached(chunk->GetLocation()->GetType() == ELocationType::Cache);
    info.set_size(chunk->GetSize());
    return info;
}

TReqHolderHeartbeat::TChunkRemoveInfo TMasterConnector::GetRemoveInfo(const TChunk* chunk)
{
    TReqHolderHeartbeat::TChunkRemoveInfo info;
    info.set_chunkid(chunk->GetId().ToProto());
    info.set_cached(chunk->GetLocation()->GetType() == ELocationType::Cache);
    return info;
}

void TMasterConnector::OnHeartbeatResponse(TProxy::TRspHolderHeartbeat::TPtr response)
{
    ScheduleHeartbeat();
    
    auto errorCode = response->GetErrorCode();
    if (errorCode != NYT::TError::OK) {
        LOG_WARNING("Error sending heartbeat to master\n%s", ~response->GetError().ToString());

        // Don't panic upon getting Timeout, TransportError or Unavailable.
        if (errorCode != NRpc::EErrorCode::Timeout && 
            errorCode != NRpc::EErrorCode::TransportError && 
            errorCode != NRpc::EErrorCode::Unavailable)
        {
            OnDisconnected();
        }

        return;
    }

    LOG_INFO("Successfully reported heartbeat to master");

    if (IncrementalHeartbeat) {
        TChunks newAddedSinceLastSuccess;
        TChunks newRemovedSinceLastSuccess;

        FOREACH (const auto& id, AddedSinceLastSuccess) {
            if (ReportedAdded.find(id) == ReportedAdded.end()) {
                newAddedSinceLastSuccess.insert(id);
            }
        }

        FOREACH (const auto& id, RemovedSinceLastSuccess) {
            if (ReportedRemoved.find(id) == ReportedRemoved.end()) {
                newRemovedSinceLastSuccess.insert(id);
            }
        }

        AddedSinceLastSuccess.swap(newAddedSinceLastSuccess);
        RemovedSinceLastSuccess.swap(newRemovedSinceLastSuccess);
    } else {
        IncrementalHeartbeat = true;
    }

    FOREACH (const auto& jobProtoId, response->jobstostop()) {
        auto jobId = TJobId::FromProto(jobProtoId);
        auto job = Replicator->FindJob(jobId);
        if (!job) {
            LOG_WARNING("Request to stop a non-existing job (JobId: %s)",
                ~jobId.ToString());
            continue;
        }

        Replicator->StopJob(~job);
    }

    FOREACH (const auto& startInfo, response->jobstostart()) {
        auto chunkId = TChunkId::FromProto(startInfo.chunkid());
        auto jobId = TJobId::FromProto(startInfo.jobid());
        
        auto chunk = ChunkStore->FindChunk(chunkId);
        if (!chunk) {
            LOG_WARNING("Job request for non-existing chunk is ignored (ChunkId: %s, JobId: %s)",
                ~chunkId.ToString(),
                ~jobId.ToString());
            continue;
        }

        Replicator->StartJob(
            EJobType(startInfo.type()),
            jobId,
            ~chunk,
            FromProto<Stroka>(startInfo.targetaddresses()));
    }
}

void TMasterConnector::OnDisconnected()
{
    HolderId = InvalidHolderId;
    Registered = false;
    IncrementalHeartbeat = false;
    ReportedAdded.clear();
    ReportedRemoved.clear();
    AddedSinceLastSuccess.clear();
    RemovedSinceLastSuccess.clear();
}

void TMasterConnector::OnChunkAdded(TChunk* chunk)
{
    if (!IncrementalHeartbeat)
        return;

    LOG_DEBUG("Registered addition of chunk (ChunkId: %s)",
        ~chunk->GetId().ToString());

    if (AddedSinceLastSuccess.find(chunk) != AddedSinceLastSuccess.end())
        return;

    if (RemovedSinceLastSuccess.find(chunk) != RemovedSinceLastSuccess.end()) {
        RemovedSinceLastSuccess.erase(chunk);
    }

    AddedSinceLastSuccess.insert(chunk);
}

void TMasterConnector::OnChunkRemoved(TChunk* chunk)
{
    if (!IncrementalHeartbeat)
        return;

    LOG_DEBUG("Registered removal of chunk (ChunkId: %s)",
        ~chunk->GetId().ToString());

    if (RemovedSinceLastSuccess.find(chunk) != RemovedSinceLastSuccess.end())
        return;

    if (AddedSinceLastSuccess.find(chunk) != AddedSinceLastSuccess.end()) {
        AddedSinceLastSuccess.erase(chunk);
    }

    RemovedSinceLastSuccess.insert(chunk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
