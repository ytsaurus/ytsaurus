#include "expiration_tracker.h"

#include "private.h"
#include "config.h"

#include <yt/server/master/cell_master/config.h>
#include <yt/server/master/cell_master/config_manager.h>
#include <yt/server/master/cell_master/hydra_facade.h>

#include <yt/server/master/chunk_server/chunk.h>

namespace NYT::NChunkServer {

using namespace NConcurrency;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;

///////////////////////////////////////////////////////////////////////////////

TExpirationTracker::TExpirationTracker(NCellMaster::TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

void TExpirationTracker::Start()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YT_LOG_INFO("Started registering staged chunk expiration (Count: %v)",
        ExpiredChunks_.size());
    for (auto* chunk : ExpiredChunks_) {
        if (auto expirationTime = chunk->GetExpirationTime()) {
            RegisterChunkExpiration(chunk, expirationTime);
        }
    }
    ExpiredChunks_.clear();
    YT_LOG_INFO("Finished registering staged chunk expiration");

    YT_VERIFY(!CheckExecutor_);
    CheckExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::ChunkExpirationTracker),
        BIND(&TExpirationTracker::OnCheck, MakeWeak(this)));
    CheckExecutor_->Start();

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->SubscribeConfigChanged(DynamicConfigChangedCallback_);
    OnDynamicConfigChanged();
}

void TExpirationTracker::Stop()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->UnsubscribeConfigChanged(DynamicConfigChangedCallback_);

    CheckExecutor_.Reset();
}

void TExpirationTracker::Clear()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    ExpirationMap_.clear();
    ExpiredChunks_.clear();
}

void TExpirationTracker::ScheduleExpiration(TChunk* chunk)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (chunk->GetExpirationIterator()) {
        UnregisterChunkExpiration(chunk);
    }

    auto expirationTime = chunk->GetExpirationTime();
    YT_VERIFY(expirationTime);
    YT_LOG_DEBUG_UNLESS(IsRecovery(), "Chunk expiration time set (ChunkId: %v, ExpirationTime: %v)",
        chunk->GetId(),
        expirationTime);
    RegisterChunkExpiration(chunk, expirationTime);
}

// NB: this is called when
//   - either the chunk expired (and unstaged by us),
//   - or the chunk was confirmed,
//   - or the chunk was explicitly unstaged (by user request),
//   - or the staging transaction was finished.
// Thus, the method must be safe to call multiple times - even when the chunk is
// no longer registered for expiration.
void TExpirationTracker::CancelExpiration(TChunk* chunk)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (chunk->GetExpirationIterator()) {
        UnregisterChunkExpiration(chunk);
    }

    // NB: Typically missing.
    ExpiredChunks_.erase(chunk);
}

void TExpirationTracker::RegisterChunkExpiration(TChunk* chunk, TInstant expirationTime)
{
    if (ExpiredChunks_.find(chunk) == ExpiredChunks_.end()) {
        auto it = ExpirationMap_.emplace(expirationTime, chunk);
        chunk->SetExpirationIterator(it);
    }
}

void TExpirationTracker::UnregisterChunkExpiration(TChunk* chunk)
{
    ExpirationMap_.erase(*chunk->GetExpirationIterator());
    chunk->SetExpirationIterator(std::nullopt);
}

void TExpirationTracker::OnCheck()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    if (!hydraManager->IsActiveLeader()) {
        return;
    }

    NProto::TReqUnstageExpiredChunks request;

    auto now = TInstant::Now();
    while (!ExpirationMap_.empty() && request.chunk_ids_size() < GetDynamicConfig()->MaxExpiredChunksUnstagesPerCommit) {
        auto [expirationTime, chunk] = *ExpirationMap_.begin();
        YT_ASSERT(chunk->GetExpirationIterator() == ExpirationMap_.begin());

        if (expirationTime > now) {
            break;
        }

        ToProto(request.add_chunk_ids(), chunk->GetId());
        UnregisterChunkExpiration(chunk);
        YT_VERIFY(ExpiredChunks_.insert(chunk).second);
    }

    if (request.chunk_ids_size() == 0) {
        return;
    }

    YT_LOG_DEBUG("Starting unstaging commit for expired chunks (Count: %v)",
        request.chunk_ids_size());

    CreateMutation(hydraManager, request)
        ->CommitAndLog(Logger);
}

bool TExpirationTracker::IsRecovery() const
{
    return Bootstrap_->GetHydraFacade()->GetHydraManager()->IsRecovery();
}

const TDynamicChunkManagerConfigPtr& TExpirationTracker::GetDynamicConfig()
{
    const auto& configManager = Bootstrap_->GetConfigManager();
    return configManager->GetConfig()->ChunkManager;
}

void TExpirationTracker::OnDynamicConfigChanged()
{
    if (CheckExecutor_) {
        CheckExecutor_->SetPeriod(GetDynamicConfig()->ExpirationCheckPeriod);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

