#include "chunk_creation_time_histogram_builder.h"

#include "private.h"

#include "chunk.h"
#include "chunk_manager.h"
#include "config.h"
#include "master_cell_chunk_statistics_collector.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/epoch_history_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/chunk_server/proto/master_cell_chunk_statistics_collector.pb.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;
using namespace NHydra;
using namespace NProfiling;
using namespace NProto;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkCreationTimeHistogramBuilder
    : public IMasterCellChunkStatisticsPieceCollector
{
public:
    explicit TChunkCreationTimeHistogramBuilder(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    void TransientClear() noexcept override
    {
        Delta_.clear();
    }

    void PersistentClear() noexcept override
    {
        Bounds_ = GetDynamicConfig()->CreationTimeHistogramBucketBounds;
        // TODO(gritukan): Fix dynamic config manager and make sure that bounds are never empty.
        if (Bounds_.empty()) {
            Bounds_ = {TInstant::Zero()};
        }

        InitializeHistogram();
    }

    void OnChunkCreated(TChunk* chunk) noexcept override
    {
        YT_ASSERT(!chunk->IsJournal());
        Histogram_.Add(GetCreationTime(chunk).MillisecondsFloat());
    }

    void OnChunkDestroyed(TChunk* chunk) noexcept override
    {
        YT_ASSERT(!chunk->IsJournal());
        Histogram_.Remove(GetCreationTime(chunk).MillisecondsFloat());
    }

    void OnChunkScan(TChunk* chunk) noexcept override
    {
        YT_ASSERT(Delta_.size() == Bounds_.size() + 1);
        YT_ASSERT(!chunk->IsJournal());

        auto estimatedCreationTime = GetCreationTime(chunk);
        auto bucketIndex = std::upper_bound(Bounds_.begin(), Bounds_.end(), estimatedCreationTime) - Bounds_.begin();
        ++Delta_[bucketIndex];
    }

    bool ScanNeeded() const noexcept override
    {
        const auto& newBounds = GetDynamicConfig()->CreationTimeHistogramBucketBounds;

        return newBounds != Bounds_;
    }

    bool FillUpdateRequest(TReqUpdateMasterCellChunkStatistics& request) const override
    {
        if (AllOf(Delta_, [] (int delta) { return delta == 0; })) {
            return false;
        }

        auto* creationTimeHistogram = request.mutable_creation_time_histogram();
        ToProto(creationTimeHistogram->mutable_delta(), Delta_);

        return true;
    }

    void OnAfterUpdateRequestFilled() noexcept override
    {
        ZeroDelta();
    }

    void OnScanScheduled(bool isLeader) override
    {
        Bounds_ = GetDynamicConfig()->CreationTimeHistogramBucketBounds;
        InitializeHistogram();

        if (isLeader) {
            ZeroDelta();
        }
    }

    void OnUpdateStatistics(const TReqUpdateMasterCellChunkStatistics& request) noexcept override
    {
        if (!request.has_creation_time_histogram()) {
            return;
        }

        const auto& delta = request.creation_time_histogram().delta();
        if (delta.size() != std::ssize(Bounds_) + 1) {
            YT_LOG_ALERT(
                "Chunk creation time histogram bounds were changed during master cell statistics update "
                "(DeltaSize: %v, BoundsSize: %v)",
                delta.size(),
                Bounds_.size());
            return;
        }

        auto snapshot = Histogram_.GetSnapshot();
        for (int i = 0; i < delta.size(); ++i) {
            snapshot.Values[i] += delta[i];
        }
        Histogram_.LoadSnapshot(std::move(snapshot));
    }

    void Save(NCellMaster::TSaveContext& context) const override
    {
        using NYT::Save;

        Save(context, Bounds_);

        {
            auto snapshot = Histogram_.GetSnapshot();
            Save(context, snapshot.Bounds);
            Save(context, snapshot.Values);
        }
    }

    void Load(NCellMaster::TLoadContext& context) override
    {
        using NYT::Load;

        Load(context, Bounds_);
        // COMPAT(gritukan): EMasterReign::FixChunkCreationTimeHistograms
        YT_VERIFY(!Bounds_.empty());

        InitializeHistogram();

        THistogramSnapshot snapshot;
        Load(context, snapshot.Bounds);
        // COMPAT(babenko)
        if (context.GetVersion() >= EMasterReign::Int64InHistogramSnapshot) {
            Load(context, snapshot.Values);
        } else {
            auto values = NYT::Load<std::vector<int>>(context);
            snapshot.Values = {values.begin(), values.end()};
        }
        Histogram_.LoadSnapshot(std::move(snapshot));
    }

private:
    TBootstrap* const Bootstrap_;

    // Persistent.
    std::vector<TInstant> Bounds_;
    TGaugeHistogram Histogram_;

    // Transient.
    std::vector<int> Delta_;

    TInstant GetCreationTime(TChunk* chunk)
    {
        const auto& epochHistoryManager = Bootstrap_->GetEpochHistoryManager();

        // NB: Estimated mutation start time is used because mutation can be not
        // finished.
        return epochHistoryManager->GetEstimatedCreationTime(
            chunk->GetId(),
            HasMutationContext()
                ? GetCurrentMutationContext()->GetTimestamp()
                : NProfiling::GetInstant()).first;
    }

    const TDynamicMasterCellChunkStatisticsCollectorConfigPtr& GetDynamicConfig() const noexcept
    {
        const auto& config = Bootstrap_->GetConfigManager()->GetConfig();
        return config->ChunkManager->MasterCellChunkStatisticsCollector;
    }

    void InitializeHistogram()
    {
        std::vector<double> bounds(Bounds_.size());
        std::transform(
            Bounds_.begin(),
            Bounds_.end(),
            bounds.begin(),
            std::mem_fn(&TInstant::MillisecondsFloat));

        Histogram_ = ChunkServerHistogramProfiler()
            .WithGlobal()
            .WithTag("cell_tag", ToString(Bootstrap_->GetCellTag()))
            .GaugeHistogram("/chunk_creation_time_histogram", bounds);
    }

    void ZeroDelta() noexcept
    {
        Delta_.assign(Bounds_.size() + 1, 0);
    }
};

////////////////////////////////////////////////////////////////////////////////


IMasterCellChunkStatisticsPieceCollectorPtr CreateChunkCreationTimeHistogramBuilder(
    TBootstrap* bootstrap)
{
    return New<TChunkCreationTimeHistogramBuilder>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
