#include "master_cell_chunk_statistics_collector.h"

#include "private.h"

#include "chunk.h"
#include "chunk_manager.h"
#include "chunk_scanner.h"
#include "config.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/chunk_server/proto/master_cell_chunk_statistics_collector.pb.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NHydra;
using namespace NProfiling;
using namespace NProto;

using NYT::ToProto;

static const auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TMasterCellChunkStatisticsCollector
    : public IMasterCellChunkStatisticsCollector
    , public TMasterAutomatonPart
{
public:
    TMasterCellChunkStatisticsCollector(
        TBootstrap* bootstrap,
        std::vector<IMasterCellChunkStatisticsPieceCollectorPtr> statisticsPieceCollectors)
        : TMasterAutomatonPart(
            bootstrap,
            EAutomatonThreadQueue::MasterCellChunkStatisticsCollector)
        , StatisticsPieceCollectors_(std::move(statisticsPieceCollectors))
        , ChunkScanner_(/*journal*/ false)
    {
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "MasterCellChunkStatisticsCollector",
            BIND(&TMasterCellChunkStatisticsCollector::Save, Unretained(this)));
        RegisterLoader(
            "MasterCellChunkStatisticsCollector",
            BIND(&TMasterCellChunkStatisticsCollector::Load, Unretained(this)));

        RegisterMethod(BIND_NO_PROPAGATE(
            &TMasterCellChunkStatisticsCollector::HydraUpdateMasterCellChunkStatistics,
            Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(
            &TMasterCellChunkStatisticsCollector::HydraRecalculateMasterCellChunkStatistics,
            Unretained(this)));

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(
            BIND_NO_PROPAGATE(&TMasterCellChunkStatisticsCollector::OnDynamicConfigChanged, Unretained(this)));
    }

    void OnChunkCreated(TChunk* chunk) override
    {
        YT_VERIFY(HasMutationContext());

        if (chunk->IsJournal()) {
            return;
        }

        for (const auto& statisticsPieceCollector : StatisticsPieceCollectors_) {
            statisticsPieceCollector->OnChunkCreated(chunk);
        }

        if (IsLeader()) {
            // NB: Despite the fact that running scanner will not see this chunk
            // it is necessary to mark the chunk as scanned in order to properly
            // account it in case of this chunk removal before global scan is
            // finished.
            // See `OnChunkDestroyed()`.
            MarkChunkAsScanned(chunk);
        }
    }

    void OnChunkDestroyed(TChunk* chunk) override
    {
        YT_VERIFY(HasMutationContext());

        if (chunk->IsJournal()) {
            return;
        }

        // NB: This chunk have to be unconditionally removed from statistics.
        // If this chunk has not been scanned yet then global chunk scan is
        // still running. In this case there will be next report in which extra
        // +1 chunk will be reported in order to compensate this chunk removal.
        for (const auto& statisticsPieceCollector : StatisticsPieceCollectors_) {
            statisticsPieceCollector->OnChunkDestroyed(chunk);
        }

        ChunkScanner_.OnChunkDestroyed(chunk);

        if (IsLeader() && Running_ && !IsChunkScanned(chunk)) {
            // NB: Chunk scan is running and this chunk has not been taken into
            // account yet. We have to correct removal of this chunk from
            // statistics here.
            for (const auto& statisticsPieceCollector : StatisticsPieceCollectors_) {
                statisticsPieceCollector->OnChunkScan(chunk);
            }
        }
    }

protected:
    void Clear() override
    {
        Running_ = false;

        for (const auto& statisticsPieceCollector : StatisticsPieceCollectors_) {
            statisticsPieceCollector->PersistentClear();
        }

        TransientClear();
    }

    void OnLeaderActive() override
    {
        TMasterAutomatonPart::OnLeaderActive();
        // NB: After epoch is changed all chunk scan flags reset to zeros. So
        // all chunks are scanned by default.
        // See TChunk::MaybeResetObsoleteEpochData().
        ScannedFlagValue_ = false;

        if (Running_) {
            // Reschedule interrupted global chunk scan.
            YT_UNUSED_FUTURE(CreateMutation(
                Bootstrap_->GetHydraFacade()->GetHydraManager(),
                TReqRecalculateMasterCellChunkStatistics{})
                ->CommitAndLog(Logger));
        }

        // NB: It is important to start executor only after ScheduleScan() is
        // called. Otherwise, Bounds_ and HistogramDelta_ will not be
        // initialized properly.
    }

    void OnStopLeading() override
    {
        TMasterAutomatonPart::OnStopLeading();
        TransientClear();
    }

    void OnAfterSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        for (const auto& statisticsPieceCollector : StatisticsPieceCollectors_) {
            statisticsPieceCollector->OnAfterSnapshotLoaded();
        }
    }

private:
    std::vector<IMasterCellChunkStatisticsPieceCollectorPtr> StatisticsPieceCollectors_;

    // Persistent fields.

    bool Running_ = false;

    // Transient fields.

    // NB: We cannot mark scanned chunks just with `true` flag because of
    // multiple global chunk scans per epoch. Instead of this we mark chunks
    // with |ScannedFlagValue_|. When one global chunk scan is finished and the
    // next needs to be scheduled it is enough just to invert this flag to mark
    // all chunks as unscanned.
    bool ScannedFlagValue_;
    TGlobalChunkScanner ChunkScanner_;

    TPeriodicExecutorPtr ChunkScanExecutor_;

    void TransientClear()
    {
        if (ChunkScanExecutor_) {
            YT_UNUSED_FUTURE(ChunkScanExecutor_->Stop());
            ChunkScanExecutor_.Reset();
        }
        StopScanner();

        ScannedFlagValue_ = false;

        for (const auto& statisticsPieceCollector : StatisticsPieceCollectors_) {
            statisticsPieceCollector->TransientClear();
        }
    }

    void StartScanner()
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        for (int shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
            ChunkScanner_.Start(chunkManager->GetGlobalBlobChunkScanDescriptor(shardIndex));
        }
    }

    void StopScanner()
    {
        for (int shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
            ChunkScanner_.Stop(shardIndex);
        }
    }

    void StartExecutorIfNeeded()
    {
        YT_ASSERT(IsLeader());

        if (ChunkScanExecutor_) {
            return;
        }

        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        const auto& invoker = hydraFacade->GetEpochAutomatonInvoker(
            EAutomatonThreadQueue::MasterCellChunkStatisticsCollector);
        ChunkScanExecutor_ = New<TPeriodicExecutor>(
            invoker,
            BIND(&TMasterCellChunkStatisticsCollector::OnChunkScan, MakeWeak(this)),
            GetDynamicConfig()->ChunkScanPeriod);
        ChunkScanExecutor_->Start();
    }

    void ScheduleScan()
    {
        YT_VERIFY(HasMutationContext());

        Running_ = true;

        for (const auto& statisticsPieceCollector : StatisticsPieceCollectors_) {
            statisticsPieceCollector->OnScanScheduled(IsLeader());
        }

        if (IsLeader()) {
            // Mark all chunks as not scanned.
            ScannedFlagValue_ = !ScannedFlagValue_;

            StopScanner();
            StartScanner();

            StartExecutorIfNeeded();
        }
    }

    void MarkChunkAsScanned(TChunk* chunk)
    {
        if (ScannedFlagValue_) {
            chunk->SetScanFlag(EChunkScanKind::GlobalStatisticsCollector);
        } else {
            chunk->ClearScanFlag(EChunkScanKind::GlobalStatisticsCollector);
        }
    }

    bool IsChunkScanned(const TChunk* chunk)
    {
        return chunk->GetScanFlag(EChunkScanKind::GlobalStatisticsCollector) == ScannedFlagValue_;
    }

    const TDynamicMasterCellChunkStatisticsCollectorConfigPtr& GetDynamicConfig()
    {
        const auto& config = Bootstrap_->GetConfigManager()->GetConfig();
        return config->ChunkManager->MasterCellChunkStatisticsCollector;
    }

    void OnChunkScan()
    {
        YT_ASSERT(IsLeader());

        if (!Running_) {
            return;
        }

        auto maxChunksPerScan = GetDynamicConfig()->MaxChunksPerScan;
        for (int i = 0; i < maxChunksPerScan && ChunkScanner_.HasUnscannedChunk(); ++i) {
            auto* chunk = ChunkScanner_.DequeueChunk();
            if (!chunk || IsChunkScanned(chunk)) {
                continue;
            }

            MarkChunkAsScanned(chunk);
            for (const auto& collector : StatisticsPieceCollectors_) {
                collector->OnChunkScan(chunk);
            }
        }

        TReqUpdateMasterCellChunkStatistics mutationRequest;

        bool emptyUpdate = true;

        for (const auto& collector : StatisticsPieceCollectors_) {
            emptyUpdate = emptyUpdate && !collector->FillUpdateRequest(mutationRequest);
        }

        // NB: If all chunks have been scanned we need to report the end of scan.
        if (ChunkScanner_.HasUnscannedChunk() && emptyUpdate) {
            return;
        }

        for (const auto& collector : StatisticsPieceCollectors_) {
            collector->OnAfterUpdateRequestFilled();
        }

        mutationRequest.set_last_batch(!ChunkScanner_.HasUnscannedChunk());

        YT_UNUSED_FUTURE(CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), mutationRequest)
            ->CommitAndLog(Logger));

        YT_LOG_DEBUG("Master cell chunk statistics updated (GlobalScanFinished: %v)",
            !ChunkScanner_.HasUnscannedChunk());
    }

    void HydraRecalculateMasterCellChunkStatistics(TReqRecalculateMasterCellChunkStatistics* /*request*/)
    {
        YT_VERIFY(HasMutationContext());

        ScheduleScan();
    }

    void HydraUpdateMasterCellChunkStatistics(TReqUpdateMasterCellChunkStatistics* request)
    {
        YT_VERIFY(HasMutationContext());

        YT_VERIFY(Running_);

        for (const auto& collector : StatisticsPieceCollectors_) {
            collector->OnUpdateStatistics(*request);
        }

        if (request->last_batch()) {
            Running_ = false;
        }

        MaybeScheduleScan();
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr oldClusterConfig)
    {
        const auto& oldConfig = oldClusterConfig->ChunkManager->MasterCellChunkStatisticsCollector;
        const auto& config = GetDynamicConfig();

        if (ChunkScanExecutor_ && oldConfig->ChunkScanPeriod != config->ChunkScanPeriod) {
            ChunkScanExecutor_->SetPeriod(config->ChunkScanPeriod);
        }

        // Scan must not be scheduled during snapshot loading.
        if (HasMutationContext()) {
            MaybeScheduleScan();
        }
    }

    void MaybeScheduleScan()
    {
        YT_VERIFY(HasMutationContext());

        if (Running_) {
            // Running global chunk scan cannot be stopped without leader epoch change.
            return;
        }

        bool scanNeeded = false;
        for (const auto& collector : StatisticsPieceCollectors_) {
            if (collector->ScanNeeded()) {
                scanNeeded = true;
                break;
            }
        }

        if (scanNeeded) {
            ScheduleScan();
        }
    }

    void Save(NCellMaster::TSaveContext& context) const
    {
        using NYT::Save;

        // TODO(kvk1920): Use TSizeSerializer.
        Save<size_t>(context, StatisticsPieceCollectors_.size());
        for (const auto& statisticsPieceCollector : StatisticsPieceCollectors_) {
            statisticsPieceCollector->Save(context);
        }

        Save(context, Running_);
    }

    void Load(NCellMaster::TLoadContext& context)
    {
        if (context.GetVersion() < EMasterReign::MasterCellChunkStatisticsCollector) {
            Running_ = false;
            return;
        }

        using NYT::Load;

        auto statisticsPieceCollectorCount = Load<size_t>(context);
        YT_VERIFY(statisticsPieceCollectorCount == StatisticsPieceCollectors_.size());

        for (const auto& collector : StatisticsPieceCollectors_) {
            collector->Load(context);
        }

        Running_ = Load<bool>(context);
    }
};

////////////////////////////////////////////////////////////////////////////////

IMasterCellChunkStatisticsCollectorPtr CreateMasterCellChunkStatisticsCollector(
    TBootstrap* bootstrap,
    std::vector<IMasterCellChunkStatisticsPieceCollectorPtr> collectors)
{
    return New<TMasterCellChunkStatisticsCollector>(bootstrap, std::move(collectors));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
