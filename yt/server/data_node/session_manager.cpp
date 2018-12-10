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

#include <yt/client/chunk_client/proto/chunk_meta.pb.h>
#include <yt/client/chunk_client/chunk_replica.h>
#include <yt/ytlib/chunk_client/file_writer.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/misc/fs.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/concurrency/periodic_executor.h>

namespace NYT::NDataNode {

using namespace NRpc;
using namespace NObjectClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NCellNode;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

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
}

ISessionPtr TSessionManager::FindSession(const TSessionId& sessionId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    YCHECK(sessionId.MediumIndex != AllMediaIndex);

    auto it = SessionMap_.find(sessionId);
    return it == SessionMap_.end() ? nullptr : it->second;
}

ISessionPtr TSessionManager::GetSessionOrThrow(const TSessionId& sessionId)
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

TSessionManager::TSessionPtrList TSessionManager::GetSessionsOrThrow(const TChunkId& chunkId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TSessionPtrList result;

    for (auto mediumIndex = 0; mediumIndex < MaxMediumCount; ++mediumIndex) {
        auto session = FindSession(TSessionId(chunkId, mediumIndex));
        if (session) {
            result.emplace_back(std::move(session));
        }
    }

    if (result.empty()) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::NoSuchSession,
            "No session found for chunk %v",
            chunkId);
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

    if (DisableWriteSessions_) {
        auto error = TError("Write sessions are disabled");
        LOG_ERROR(error);
        THROW_ERROR(error);
    }

    auto session = CreateSession(sessionId, options);

    session->SubscribeFinished(
        BIND(&TSessionManager::OnSessionFinished, MakeStrong(this), MakeWeak(session))
            .Via(Bootstrap_->GetControlInvoker()));

    RegisterSession(session);

    return session;
}

ISessionPtr TSessionManager::CreateSession(
    const TSessionId& sessionId,
    const TSessionOptions& options)
{
    auto chunkType = TypeFromId(DecodeChunkId(sessionId.ChunkId).Id);

    const auto& chunkStore = Bootstrap_->GetChunkStore();
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
    if (!session) {
        return;
    }

    LOG_INFO("Session lease expired (ChunkId: %v)",
        sessionId);

    session->Cancel(TError("Session lease expired"));
}

void TSessionManager::OnSessionFinished(const TWeakPtr<ISession>& session, const TError& /*error*/)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto session_ = session.Lock();
    if (!session_) {
        return;
    }

    LOG_INFO("Session finished (ChunkId: %v)",
        session_->GetId());

    UnregisterSession(session_);
}

int TSessionManager::GetSessionCount(ESessionType type)
{
    VERIFY_THREAD_AFFINITY_ANY();

    int result = 0;
    const auto& chunkStore = Bootstrap_->GetChunkStore();
    for (const auto& location : chunkStore->Locations()) {
        result += location->GetSessionCount(type);
    }
    return result;
}

void TSessionManager::RegisterSession(const ISessionPtr& session)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    YCHECK(SessionMap_.emplace(session->GetId(), session).second);
    session->GetStoreLocation()->UpdateSessionCount(session->GetType(), +1);
}

void TSessionManager::UnregisterSession(const ISessionPtr& session)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    YCHECK(SessionMap_.erase(session->GetId()) == 1);
    session->GetStoreLocation()->UpdateSessionCount(session->GetType(), -1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
