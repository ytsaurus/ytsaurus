#include "stdafx.h"
#include "master_connector.h"

#include <util/system/hostname.h>

#include "../rpc/client.h"
#include "../meta_state/cell_channel.h"
#include "../misc/delayed_invoker.h"
#include "../misc/serialize.h"
#include "../misc/string.h"

namespace NYT {
namespace NChunkHolder {

using namespace NMetaState;
using namespace NChunkServer::NProto;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

TMasterConnector::TMasterConnector(
    const TConfig& config,
    TChunkStore::TPtr chunkStore,
    TSessionManager::TPtr sessionManager,
    TReplicator::TPtr replicator,
    IInvoker::TPtr serviceInvoker)
    : Config(config)
    , ChunkStore(chunkStore)
    , SessionManager(sessionManager)
    , Replicator(replicator)
    , ServiceInvoker(serviceInvoker)
    , Registered(false)
    , IncrementalHeartbeat(false)
    , HolderId(InvalidHolderId)
{
    YASSERT(~chunkStore != NULL);
    YASSERT(~sessionManager != NULL);
    YASSERT(~replicator != NULL);
    YASSERT(~serviceInvoker != NULL);

    auto channel = CreateCellChannel(Config.Masters);
    Proxy.Reset(new TProxy(~channel));
    Proxy->SetTimeout(Config.RpcTimeout);

    Address = Sprintf("%s:%d", ~HostName(), Config.Port);

    ChunkStore->ChunkAdded().Subscribe(FromMethod(
        &TMasterConnector::OnChunkAdded,
        TPtr(this)));
    ChunkStore->ChunkRemoved().Subscribe(FromMethod(
        &TMasterConnector::OnChunkRemoved,
        TPtr(this)));

    LOG_INFO("Chunk holder address is %s, master addresses are [%s]",
        ~Address,
        ~JoinToString(Config.Masters.Addresses));

    OnHeartbeat();
}

void TMasterConnector::ScheduleHeartbeat()
{
    TDelayedInvoker::Get()->Submit(
        FromMethod(&TMasterConnector::OnHeartbeat, TPtr(this))->Via(ServiceInvoker),
        Config.HeartbeatPeriod);
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
    *request->MutableStatistics() = statistics.ToProto();

    request->SetAddress(Address);

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

    if (Config.MaxChunksSpace >= 0) {
        result.AvailableSpace = Max((i64) 0, Config.MaxChunksSpace - result.UsedSpace);
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

        LOG_WARNING("Error registering at master (Error: %s)",
            ~response->GetError().ToString());
        return;
    }

    HolderId = response->GetHolderId();
    Registered = true;
    IncrementalHeartbeat = false;

    LOG_INFO("Successfully registered at master (HolderId: %d)",
        HolderId);
}

void TMasterConnector::SendHeartbeat()
{
    auto request = Proxy->HolderHeartbeat();

    YASSERT(HolderId != InvalidHolderId);
    request->SetHolderId(HolderId);

    auto statistics = ComputeStatistics();
    *request->MutableStatistics() = statistics.ToProto();

    if (IncrementalHeartbeat) {
        ReportedAdded = AddedSinceLastSuccess;
        ReportedRemoved = RemovedSinceLastSuccess;

        FOREACH (const auto& chunk, ReportedAdded) {
            *request->AddAddedChunks() = GetInfo(~chunk);
        }

        FOREACH (const auto& chunk, ReportedRemoved) {
            request->AddRemovedChunks(chunk->GetId().ToProto());
        }
    } else {
        FOREACH (const auto& chunk, ChunkStore->GetChunks()) {
            *request->AddAddedChunks() = GetInfo(~chunk);
        }
    }

    FOREACH (const auto& job, Replicator->GetAllJobs()) {
        auto* info = request->AddJobs();
        info->SetJobId(job->GetJobId().ToProto());
        info->SetState(job->GetState());
    }

    request->Invoke()->Subscribe(
        FromMethod(&TMasterConnector::OnHeartbeatResponse, TPtr(this))
        ->Via(ServiceInvoker));

    LOG_DEBUG("Heartbeat sent (%s, AddedChunks: %d, RemovedChunks: %d, Jobs: %d)",
        ~statistics.ToString(),
        static_cast<int>(request->AddedChunksSize()),
        static_cast<int>(request->RemovedChunksSize()),
        static_cast<int>(request->JobsSize()));
}

void TMasterConnector::OnHeartbeatResponse(TProxy::TRspHolderHeartbeat::TPtr response)
{
    ScheduleHeartbeat();
    
    auto errorCode = response->GetErrorCode();
    if (errorCode != NRpc::EErrorCode::OK) {
        LOG_WARNING("Error sending heartbeat to master (Error: %s)",
            ~response->GetError().ToString());

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

    FOREACH (const auto& jobProtoId, response->GetJobsToStop()) {
        auto jobId = TJobId::FromProto(jobProtoId);
        auto job = Replicator->FindJob(jobId);
        if (~job == NULL) {
            LOG_WARNING("Request to stop a non-existing job (JobId: %s)",
                ~jobId.ToString());
            continue;
        }

        Replicator->StopJob(job);
    }

    FOREACH (const auto& startInfo, response->GetJobsToStart()) {
        auto chunkId = TChunkId::FromProto(startInfo.GetChunkId());
        auto jobId = TJobId::FromProto(startInfo.GetJobId());
        
        auto chunk = ChunkStore->FindChunk(chunkId);
        if (~chunk == NULL) {
            LOG_WARNING("Job request for non-existing chunk is ignored (ChunkId: %s, JobId: %s)",
                ~chunkId.ToString(),
                ~jobId.ToString());
            continue;
        }

        Replicator->StartJob(
            EJobType(startInfo.GetType()),
            jobId,
            chunk,
            FromProto<Stroka>(startInfo.GetTargetAddresses()));
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

NChunkServer::NProto::TChunkInfo TMasterConnector::GetInfo(TChunk* chunk)
{
    NChunkServer::NProto::TChunkInfo result;
    result.SetId(chunk->GetId().ToProto());
    result.SetSize(chunk->GetSize());
    auto meta = chunk->GetMasterMeta();
    result.SetMasterMeta(meta.Begin(), meta.Size());
    return result;
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
