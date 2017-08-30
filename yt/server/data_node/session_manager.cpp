#include "session_manager.h"
#include "private.h"
#include "blob_chunk.h"
#include "blob_session.h"
#include "chunk_block_manager.h"
#include "chunk_store.h"
#include "config.h"
#include "journal_session.h"
#include "location.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/ytlib/chunk_client/chunk_meta.pb.h>
#include <yt/ytlib/chunk_client/chunk_replica.h>
#include <yt/ytlib/chunk_client/file_writer.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/core/misc/fs.h>

#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/timing.h>

namespace NYT {
namespace NDataNode {

using namespace NRpc;
using namespace NObjectClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NCellNode;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;
static const auto& Profiler = DataNodeProfiler;

////////////////////////////////////////////////////////////////////////////////

TSessionManager::TSessionManager(
    TDataNodeConfigPtr config,
    TBootstrap* bootstrap)
    : Config_(config)
    , Bootstrap_(bootstrap)
{
    YCHECK(config);
    YCHECK(bootstrap);
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetControlInvoker(), ControlThread);

    auto* profilingManager = NProfiling::TProfileManager::Get();
    for (auto type : TEnumTraits<ESessionType>::GetDomainValues()) {
        PerTypeSessionCounters_[type] = NProfiling::TSimpleCounter(
            "/session_count",
            {profilingManager->RegisterTag("type", type)});
    }
}

ISessionPtr TSessionManager::FindSession(const TSessionId& sessionId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    YCHECK(sessionId.MediumIndex != AllMediaIndex);

    auto it = SessionMap_.find(sessionId);
    return it == SessionMap_.end() ? nullptr : it->second;
}

ISessionPtr TSessionManager::GetSession(const TSessionId& sessionId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto session = FindSession(sessionId);
    if (!session) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::NoSuchSession,
            "Session %v is invalid or expired",
            sessionId);
    }
    return session;
}

TSessionManager::TSessionPtrList TSessionManager::FindSessions(const TSessionId& sessionId)
{
    TSessionPtrList result;

    if (sessionId.MediumIndex != AllMediaIndex) {
        auto session = FindSession(sessionId);
        if (session) {
            result.emplace_back(std::move(session));
        }
    } else {
        const auto& chunkId = sessionId.ChunkId;

        for (int mediumIndex = 0; mediumIndex < MaxMediumCount; ++mediumIndex) {
            auto session = FindSession(TSessionId(chunkId, mediumIndex));
            if (session) {
                result.emplace_back(std::move(session));
            }
        }
    }

    return result;
}

TSessionManager::TSessionPtrList TSessionManager::GetSessions(const TSessionId& sessionId)
{
    auto result = FindSessions(sessionId);
    if (result.empty()) {
       THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::NoSuchSession,
            "Session %v is invalid or expired",
            sessionId);
    }
    return result;
}

ISessionPtr TSessionManager::StartSession(
    const TSessionId& sessionId,
    const TSessionOptions& options)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (SessionMap_.size() >= Config_->MaxWriteSessions) {
        auto error = TError("Maximum concurrent write session limit %v has been reached",
            Config_->MaxWriteSessions);
        LOG_ERROR(error);
        THROW_ERROR(error);
    }

    auto session = CreateSession(sessionId, options);

    session->SubscribeFinished(
        BIND(&TSessionManager::OnSessionFinished, MakeStrong(this), Unretained(session.Get()))
            .Via(Bootstrap_->GetControlInvoker()));

    RegisterSession(session);

    return session;
}

ISessionPtr TSessionManager::CreateSession(
    const TSessionId& sessionId,
    const TSessionOptions& options)
{
    auto chunkType = TypeFromId(DecodeChunkId(sessionId.ChunkId).Id);

    auto chunkStore = Bootstrap_->GetChunkStore();
    auto location = chunkStore->GetNewChunkLocation(sessionId, options);

    auto lease = TLeaseManager::CreateLease(
        Config_->SessionTimeout,
        BIND(&TSessionManager::OnSessionLeaseExpired, MakeStrong(this), sessionId)
            .Via(Bootstrap_->GetControlInvoker()));

    ISessionPtr session;
    switch (chunkType) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
            session = New<TBlobSession>(
                Config_,
                Bootstrap_,
                sessionId,
                options,
                location,
                lease);
            break;

        case EObjectType::JournalChunk:
            session = New<TJournalSession>(
                Config_,
                Bootstrap_,
                sessionId,
                options,
                location,
                lease);
            break;
    
        default:
            THROW_ERROR_EXCEPTION("Invalid session chunk type %Qlv",
                chunkType);
    }

    return session;
}

void TSessionManager::OnSessionLeaseExpired(const TSessionId& sessionId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto session = FindSession(sessionId);
    if (!session)
        return;

    LOG_INFO("Session lease expired (ChunkId: %v)",
        sessionId);

    session->Cancel(TError("Session lease expired"));
}

void TSessionManager::OnSessionFinished(ISession* session, const TError& /*error*/)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Session finished (ChunkId: %v)",
        session->GetId());

    UnregisterSession(session);
}

int TSessionManager::GetSessionCount(ESessionType type)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return PerTypeSessionCounters_[type].GetCurrent();
}

void TSessionManager::RegisterSession(ISessionPtr session)
{
    Profiler.Increment(PerTypeSessionCounters_[session->GetType()], +1);
    YCHECK(SessionMap_.insert(std::make_pair(session->GetId(), session)).second);
}

void TSessionManager::UnregisterSession(ISessionPtr session)
{
    Profiler.Increment(PerTypeSessionCounters_[session->GetType()], -1);
    YCHECK(SessionMap_.erase(session->GetId()) == 1);
}

std::vector<ISessionPtr> TSessionManager::GetSessions()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    std::vector<ISessionPtr> result;
    for (const auto& pair : SessionMap_) {
        result.push_back(pair.second);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
