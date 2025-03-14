#include "session_manager.h"

#include "bootstrap.h"
#include "private.h"
#include "blob_chunk.h"
#include "blob_session.h"
#include "chunk_store.h"
#include "config.h"
#include "journal_session.h"
#include "location.h"
#include "nbd_session.h"

#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/library/ytprof/heap_profiler.h>

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NDataNode {

using namespace NRpc;
using namespace NObjectClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NYTProf;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TSessionManager::TSessionManager(
    TDataNodeConfigPtr config,
    IBootstrap* bootstrap)
    : Config_(std::move(config))
    , Bootstrap_(bootstrap)
    , OrchidService_(CreateOrchidService())
{
    YT_VERIFY(Config_);
    YT_VERIFY(Bootstrap_);

    DataNodeProfiler().AddFuncGauge("/write_sessions_disabled", MakeStrong(this), [this] {
        return DisableWriteSessions_.load() ? 1.0 : 0.0;
    });
}

void TSessionManager::BuildOrchid(IYsonConsumer* consumer)
{
    decltype(SessionMap_) sessionMap;
    {
        auto guard = ReaderGuard(SessionMapLock_);
        sessionMap = SessionMap_;
    }

    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("sessions")
            .BeginMap()
                .DoFor(
                    sessionMap.begin(),
                    sessionMap.end(),
                    [&] (TFluentMap fluent, auto it) {
                        const auto& session = it->second;
                        const auto& location = session->GetStoreLocation();
                        fluent
                            .Item(ToString(session->GetId()))
                            .BeginMap()
                                .Item("chunk_id").Value(session->GetChunkId())
                                .Item("type").Value(ToString(session->GetType()))
                                .Item("start_time").Value(session->GetStartTime())
                                .Item("memory_usage").Value(session->GetMemoryUsage())
                                .Item("block_count").Value(session->GetBlockCount())
                                .Item("size").Value(session->GetTotalSize())
                                .Item("location_id").Value(location->GetId())
                                .Item("location_uuid").Value(location->GetUuid())
                                .Item("intermediate_empty_block_count").Value(session->GetIntermediateEmptyBlockCount())
                                .Item("medium").Value(location->GetMediumName())
                            .EndMap();
                    })
            .EndMap()
            .Item("session_count").Value(sessionMap.size())
            .Item("disable_write_sessions").Value(DisableWriteSessions_.load())
        .EndMap();
}

IYPathServicePtr TSessionManager::CreateOrchidService()
{
    return IYPathService::FromProducer(BIND(&TSessionManager::BuildOrchid, MakeStrong(this)))
        ->Via(Bootstrap_->GetControlInvoker());
}

IYPathServicePtr TSessionManager::GetOrchidService()
{
    return OrchidService_;
}

void TSessionManager::Initialize()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    Bootstrap_->SubscribeMasterDisconnected(
        BIND_NO_PROPAGATE(&TSessionManager::OnMasterDisconnected, MakeWeak(this)));

    Bootstrap_->GetChunkStore()->SubscribeChunkRemovalScheduled(
        BIND_NO_PROPAGATE(&TSessionManager::OnChunkRemovalScheduled, MakeWeak(this)));
}

ISessionPtr TSessionManager::FindSession(TChunkId chunkId)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(SessionMapLock_);
    auto it = SessionMap_.find(chunkId);
    return it == SessionMap_.end() ? nullptr : it->second;
}

ISessionPtr TSessionManager::GetSessionOrThrow(TChunkId chunkId)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto session = FindSession(chunkId);
    if (!session) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::NoSuchSession,
            "Session %v is invalid or expired",
            chunkId);
    }
    return session;
}

ISessionPtr TSessionManager::StartSession(
    TSessionId sessionId,
    const TSessionOptions& options)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    if (!options.NbdChunkSize) {
        auto chunkCellTag = CellTagFromId(sessionId.ChunkId);
        const auto& clusterNodeMasterConnector = Bootstrap_->GetClusterNodeBootstrap()->GetMasterConnector();
        auto masterCellTags = clusterNodeMasterConnector->GetMasterCellTags();
        if (Find(masterCellTags, chunkCellTag) == masterCellTags.end()) {
            YT_LOG_ALERT("Attempt to start a write session with an unknown master cell tag (SessionId: %v, CellTag: %v)",
                sessionId,
                chunkCellTag);
            THROW_ERROR_EXCEPTION("Unknown master cell tag %v",
                chunkCellTag);
        }
    }

    auto guard = WriterGuard(SessionMapLock_);

    if (!options.NbdChunkSize) {
        if (std::ssize(SessionMap_) >= Config_->MaxWriteSessions) {
            THROW_ERROR_EXCEPTION("Maximum concurrent write session limit %v has been reached",
                Config_->MaxWriteSessions);
        }
    }

    if (SessionMap_.contains(sessionId.ChunkId)) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::SessionAlreadyExists,
            "Write session for chunk %v is already registered",
            sessionId.ChunkId);
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

    YT_UNUSED_FUTURE(session
        ->GetStoreLocation()
        ->RegisterAction(BIND([=] {
            return session->GetUnregisteredEvent();
        })));

    RegisterSession(session);

    return session;
}

ISessionPtr TSessionManager::CreateSession(
    TSessionId sessionId,
    const TSessionOptions& options)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    const auto& chunkStore = Bootstrap_->GetChunkStore();
    auto [location, lockedChunkGuard] = chunkStore->AcquireNewChunkLocation(sessionId, options);

    auto lease = TLeaseManager::CreateLease(
        Config_->SessionTimeout,
        BIND(&TSessionManager::OnSessionLeaseExpired, MakeStrong(this), sessionId.ChunkId)
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
                std::move(lockedChunkGuard),
                /*writeBlocksOptions*/ IChunkWriter::TWriteBlocksOptions{});
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
                std::move(lockedChunkGuard),
                /*writeBlocksOptions*/ IChunkWriter::TWriteBlocksOptions{});
            break;

        case EObjectType::NbdChunk:
            return New<TNbdSession>(
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

void TSessionManager::OnSessionLeaseExpired(TChunkId chunkId)
{
    YT_ASSERT_INVOKER_AFFINITY(Bootstrap_->GetStorageLightInvoker());

    auto session = FindSession(chunkId);
    if (!session) {
        return;
    }

    YT_LOG_DEBUG("Session lease expired (ChunkId: %v)",
        chunkId);

    session->Cancel(TError("Session lease expired"));
}

void TSessionManager::OnSessionFinished(const TWeakPtr<ISession>& weakSession, const TError& /*error*/)
{
    YT_ASSERT_INVOKER_AFFINITY(Bootstrap_->GetStorageLightInvoker());

    auto session = weakSession.Lock();
    if (!session) {
        return;
    }

    YT_LOG_DEBUG("Session finished (ChunkId: %v)",
        session->GetChunkId());

    {
        auto guard = WriterGuard(SessionMapLock_);
        UnregisterSession(session);
    }
}

int TSessionManager::GetSessionCount(ESessionType type)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    int result = 0;
    const auto& chunkStore = Bootstrap_->GetChunkStore();
    for (const auto& location : chunkStore->Locations()) {
        result += location->GetSessionCount(type);
    }
    return result;
}

bool TSessionManager::GetDisableWriteSessions()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return DisableWriteSessions_.load();
}

void TSessionManager::SetDisableWriteSessions(bool value)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    DisableWriteSessions_.store(value);
}

void TSessionManager::RegisterSession(const ISessionPtr& session)
{
    YT_ASSERT_SPINLOCK_AFFINITY(SessionMapLock_);

    EmplaceOrCrash(SessionMap_, session->GetChunkId(), session);
    EmplaceOrCrash(
        SessionToCreatedAt_,
        TSessionCreatedAtSortIndex{
            .ChunkId = session->GetChunkId(),
            .StartedAt = session->GetStartTime(),
        });

    session->GetStoreLocation()->UpdateSessionCount(session->GetType(), +1);
}

void TSessionManager::UnregisterSession(const ISessionPtr& session)
{
    YT_ASSERT_SPINLOCK_AFFINITY(SessionMapLock_);

    EraseOrCrash(SessionMap_, session->GetChunkId());
    EraseOrCrash(
        SessionToCreatedAt_,
        TSessionCreatedAtSortIndex{
            .ChunkId = session->GetChunkId(),
            .StartedAt = session->GetStartTime(),
        });

    session->GetStoreLocation()->UpdateSessionCount(session->GetType(), -1);
    session->UnlockChunk();
    session->OnUnregistered();
}

void TSessionManager::OnMasterDisconnected()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    decltype(SessionMap_) sessionMap;
    {
        auto guard = ReaderGuard(SessionMapLock_);
        sessionMap = SessionMap_;
    }

    for (const auto& [_, session] : sessionMap) {
        session->Cancel(TError("Node has disconnected from master"));
    }
}

void TSessionManager::OnChunkRemovalScheduled(const IChunkPtr& chunk)
{
    auto chunkId = chunk->GetId();
    if (auto session = FindSession(chunkId)) {
        session->Cancel(TError("Chunk %v is about to be removed",
            chunkId));
    }
}

void TSessionManager::CancelLocationSessions(const TChunkLocationPtr& location)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    decltype(SessionMap_) sessionMap;
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

bool TSessionManager::CanPassSessionOutOfTurn(TChunkId chunkId)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto count = Config_->MaxOutOfTurnSessions;
    if (count == 0) {
        return false;
    }

    auto guard = ReaderGuard(SessionMapLock_);

    if (!SessionMap_.contains(chunkId)) {
        return false;
    }

    auto begin = SessionToCreatedAt_.begin();
    auto end = std::ssize(SessionToCreatedAt_) < count ? SessionToCreatedAt_.end() : std::next(begin, count);
    return std::find_if(
        begin,
        end,
        [&] (const auto& index) {
            return chunkId == index.ChunkId;
        }) != SessionToCreatedAt_.end();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
