#include "session_manager.h"

#include "bootstrap.h"
#include "private.h"
#include "blob_chunk.h"
#include "blob_session.h"
#include "chunk_store.h"
#include "config.h"
#include "journal_session.h"
#include "location.h"

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>
#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NDataNode {

using namespace NRpc;
using namespace NObjectClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NClusterNode;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TSessionManager::TSessionManager(
    TDataNodeConfigPtr config,
    IBootstrap* bootstrap)
    : Config_(std::move(config))
    , Bootstrap_(bootstrap)
{
    YT_VERIFY(Config_);
    YT_VERIFY(Bootstrap_);

    DataNodeProfiler.AddFuncGauge("/write_sessions_disabled", MakeStrong(this), [this] {
        return DisableWriteSessions_.load() ? 1.0 : 0.0;
    });
}

void TSessionManager::Initialize()
{
    VERIFY_THREAD_AFFINITY_ANY();

    Bootstrap_->SubscribeMasterDisconnected(
        BIND_NO_PROPAGATE(&TSessionManager::OnMasterDisconnected, MakeWeak(this)));

    Bootstrap_->GetChunkStore()->SubscribeChunkRemovalScheduled(
        BIND_NO_PROPAGATE(&TSessionManager::OnChunkRemovalScheduled, MakeWeak(this)));
}

ISessionPtr TSessionManager::FindSession(TSessionId sessionId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_VERIFY(sessionId.MediumIndex != AllMediaIndex);

    auto guard = ReaderGuard(SessionMapLock_);
    auto it = SessionMap_.find(sessionId);
    return it == SessionMap_.end() ? nullptr : it->second;
}

ISessionPtr TSessionManager::GetSessionOrThrow(TSessionId sessionId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto session = FindSession(sessionId);
    if (!session) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::NoSuchSession,
            "Session %v is invalid or expired",
            sessionId);
    }
    return session;
}

ISessionPtr TSessionManager::StartSession(
    TSessionId sessionId,
    const TSessionOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(SessionMapLock_);

    if (std::ssize(SessionMap_) >= Config_->MaxWriteSessions) {
        THROW_ERROR_EXCEPTION("Maximum concurrent write session limit %v has been reached",
            Config_->MaxWriteSessions);
    }

    if (SessionMap_.contains(sessionId)) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::SessionAlreadyExists,
            "Write session %v is already registered",
            sessionId);
    }

    if (Bootstrap_->GetChunkStore()->FindChunk(sessionId.ChunkId, sessionId.MediumIndex)) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::ChunkAlreadyExists,
            "Chunk %v already exists",
            sessionId);
    }

    if (DisableWriteSessions_) {
        THROW_ERROR_EXCEPTION("Write sessions are disabled");
    }

    auto session = CreateSession(sessionId, options);

    session->SubscribeFinished(
        BIND(&TSessionManager::OnSessionFinished, MakeStrong(this), MakeWeak(session))
            .Via(Bootstrap_->GetStorageLightInvoker()));

    session
        ->GetStoreLocation()
        ->RegisterAction(BIND([=] {
            return session->GetUnregisteredEvent();
        }));

    RegisterSession(session);

    return session;
}

ISessionPtr TSessionManager::CreateSession(
    TSessionId sessionId,
    const TSessionOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto& chunkStore = Bootstrap_->GetChunkStore();
    auto [location, lockedChunkGuard] = chunkStore->AcquireNewChunkLocation(sessionId, options);

    auto lease = TLeaseManager::CreateLease(
        Config_->SessionTimeout,
        BIND(&TSessionManager::OnSessionLeaseExpired, MakeStrong(this), sessionId)
            .Via(Bootstrap_->GetStorageLightInvoker()));

    auto chunkType = TypeFromId(DecodeChunkId(sessionId.ChunkId).Id);
    switch (chunkType) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
            return New<TBlobSession>(
                Config_,
                Bootstrap_,
                sessionId,
                options,
                location,
                lease,
                std::move(lockedChunkGuard));
            break;

        case EObjectType::JournalChunk:
        case EObjectType::ErasureJournalChunk:
            return New<TJournalSession>(
                Config_,
                Bootstrap_,
                sessionId,
                options,
                location,
                lease,
                std::move(lockedChunkGuard));
            break;

        default:
            THROW_ERROR_EXCEPTION("Invalid session chunk type %Qlv",
                chunkType);
    }
}

void TSessionManager::OnSessionLeaseExpired(TSessionId sessionId)
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetStorageLightInvoker());

    auto session = FindSession(sessionId);
    if (!session) {
        return;
    }

    YT_LOG_DEBUG("Session lease expired (ChunkId: %v)",
        sessionId);

    session->Cancel(TError("Session lease expired"));
}

void TSessionManager::OnSessionFinished(const TWeakPtr<ISession>& weakSession, const TError& /*error*/)
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetStorageLightInvoker());

    auto session = weakSession.Lock();
    if (!session) {
        return;
    }

    YT_LOG_DEBUG("Session finished (ChunkId: %v)",
        session->GetId());

    {
        auto guard = WriterGuard(SessionMapLock_);
        UnregisterSession(session);
    }
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

bool TSessionManager::GetDisableWriteSessions()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return DisableWriteSessions_.load();
}

void TSessionManager::SetDisableWriteSessions(bool value)
{
    VERIFY_THREAD_AFFINITY_ANY();

    DisableWriteSessions_.store(value);
}

void TSessionManager::RegisterSession(const ISessionPtr& session)
{
    VERIFY_SPINLOCK_AFFINITY(SessionMapLock_);

    YT_VERIFY(SessionMap_.emplace(session->GetId(), session).second);
    session->GetStoreLocation()->UpdateSessionCount(session->GetType(), +1);
}

void TSessionManager::UnregisterSession(const ISessionPtr& session)
{
    VERIFY_SPINLOCK_AFFINITY(SessionMapLock_);

    YT_VERIFY(SessionMap_.erase(session->GetId()) == 1);
    session->GetStoreLocation()->UpdateSessionCount(session->GetType(), -1);
    session->OnUnregistered();
}

void TSessionManager::OnMasterDisconnected()
{
    VERIFY_THREAD_AFFINITY_ANY();

    THashMap<TSessionId, ISessionPtr> sessionMap;
    {
        auto guard = ReaderGuard(SessionMapLock_);
        sessionMap = SessionMap_;
    }

    for (const auto& [sessionId, session] : sessionMap) {
        session->Cancel(TError("Node has disconnected from master"));
    }
}

void TSessionManager::OnChunkRemovalScheduled(const IChunkPtr& chunk)
{
    auto sessionId = TSessionId(
        chunk->GetId(),
        chunk->GetLocation()->GetMediumDescriptor().Index);
    if (auto session = FindSession(sessionId)) {
        session->Cancel(TError("Chunk %v is about to be removed",
            chunk->GetId()));
    }
}

void TSessionManager::CancelLocationSessions(const TChunkLocationPtr& location)
{
    VERIFY_THREAD_AFFINITY_ANY();

    THashMap<TSessionId, ISessionPtr> sessionMap;
    {
        auto guard = ReaderGuard(SessionMapLock_);
        sessionMap = SessionMap_;
    }

    for (const auto& [sessionId, session] : sessionMap) {
        if (location == session->GetStoreLocation()) {
            session->Cancel(TError("Location disabled (LocationUuid: %v)", location->GetUuid()));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
