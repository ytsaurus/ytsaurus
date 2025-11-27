#include "chunk_pools_output_merger.h"

#include "chunk_pool.h"
#include "helpers.h"

#include <yt/yt/server/lib/controller_agent/structs.h>

#include <util/generic/xrange.h>

#include <util/random/random.h>

namespace NYT::NChunkPools {

using namespace NChunkClient;
using namespace NControllerAgent;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NScheduler;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TChunkPoolsOutputsMerger
    : public TChunkPoolOutputWithCountersBase
    , public TLoggerOwner
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(), Completed);
    DEFINE_SIGNAL_OVERRIDE(void(), Uncompleted);

public:
    //! Used only for persistence.
    TChunkPoolsOutputsMerger() = default;

    TChunkPoolsOutputsMerger(
        std::vector<IPersistentChunkPoolOutputPtr> chunkPools,
        TSerializableLogger logger)
        : TLoggerOwner(std::move(logger))
        , ChunkPools_(std::move(chunkPools))
        , ParentJobCounter_(New<TProgressCounter>())
        , ParentDataWeightCounter_(New<TProgressCounter>())
        , ParentRowCounter_(New<TProgressCounter>())
        , ParentDataSliceCounter_(New<TProgressCounter>())
    {
        ValidateLogger(Logger);

        for (int poolIndex : xrange(std::ssize(ChunkPools_))) {
            const auto& chunkPool = ChunkPools_[poolIndex];

            SubscribePoolPendingUpdated(poolIndex);

            chunkPool->GetJobCounter()->AddParent(ParentJobCounter_);
            chunkPool->GetDataWeightCounter()->AddParent(ParentDataWeightCounter_);
            chunkPool->GetRowCounter()->AddParent(ParentRowCounter_);
            chunkPool->GetDataSliceCounter()->AddParent(ParentDataSliceCounter_);

            const auto& jobCounter = chunkPool->GetJobCounter();
            YT_VERIFY(jobCounter->GetRunning() == 0);
            YT_VERIFY(jobCounter->GetCompletedTotal() == 0);

            YT_LOG_DEBUG("Initialized chunk pool (PoolIndex: %v)", poolIndex);
        }

        UpdateCounters();

        YT_LOG_INFO("Chunk pool output merger created (PoolCount: %v)", ChunkPools_.size());
    }

    TChunkStripeStatisticsVector GetApproximateStripeStatistics() const override
    {
        TChunkStripeStatisticsVector result;
        for (const auto& chunkPool : ChunkPools_) {
            auto poolStats = chunkPool->GetApproximateStripeStatistics();
            result.insert(result.end(), poolStats.begin(), poolStats.end());
        }

        YT_LOG_TRACE("Retrieved approximate stripe statistics (TotalStripes: %v)", result.size());
        return result;
    }

    TCookie Extract(TNodeId nodeId) override
    {
        YT_LOG_DEBUG("Extracting job (NodeId: %v)", nodeId);

        VerifyCanExtract();

        ExtractedCookie_ = static_cast<TCookie>(RandomNumber<ui32>(std::numeric_limits<TCookie>::max()));

        YT_LOG_DEBUG("Generated cookie (Cookie: %v)", ExtractedCookie_);

        UnderlyingChunkPoolCookies_.clear();
        UnderlyingChunkPoolCookies_.resize(ChunkPools_.size());

        std::vector<TChunkStripeListPtr> stripeLists;
        stripeLists.reserve(ChunkPools_.size());

        WithUpdateDisabled([&] {
            for (int poolIndex : xrange(std::ssize(ChunkPools_))) {
                const auto& chunkPool = ChunkPools_[poolIndex];

                for (auto cookie = chunkPool->Extract(nodeId); cookie != NullCookie; cookie = chunkPool->Extract(nodeId)) {
                    UnderlyingChunkPoolCookies_[poolIndex].push_back(cookie);
                    stripeLists.push_back(chunkPool->GetStripeList(cookie));
                }

                YT_LOG_DEBUG(
                    "Extracted cookies from pool (PoolIndex: %v, ExtractedCount: %v)",
                    poolIndex,
                    std::ssize(UnderlyingChunkPoolCookies_[poolIndex]));

                const auto& jobCounter = chunkPool->GetJobCounter();
                YT_VERIFY(jobCounter->GetTotal() == std::ssize(UnderlyingChunkPoolCookies_[poolIndex]));
                YT_VERIFY(jobCounter->GetTotal() == jobCounter->GetRunning());
            }
        });

        ExtractedChunkStripeList_ = MergeStripeLists(stripeLists);

        IsRunning_ = true;

        JobCounter_->AddRunning(1);

        auto statistics = ExtractedChunkStripeList_->GetAggregateStatistics();

        DataWeightCounter_->AddRunning(statistics.DataWeight);
        RowCounter_->AddRunning(statistics.RowCount);
        DataSliceCounter_->AddRunning(ExtractedChunkStripeList_->GetSliceCount());

        UpdateCounters();

        YT_LOG_DEBUG(
            "Job extracted (Cookie: %v, DataWeight: %v, RowCount: %v, SliceCount: %v)",
            ExtractedCookie_,
            statistics.DataWeight,
            statistics.RowCount,
            ExtractedChunkStripeList_->GetSliceCount());

        return ExtractedCookie_;
    }

    TChunkStripeListPtr GetStripeList(TCookie cookie) override
    {
        YT_LOG_TRACE("Getting stripe list (Cookie: %v)", cookie);

        VerifyExtractedCookieState(cookie, /*shouldBeRunning*/ true, /*shouldBeCompleted*/ false);
        YT_VERIFY(std::ssize(UnderlyingChunkPoolCookies_) == std::ssize(ChunkPools_));

        return ExtractedChunkStripeList_;
    }

    bool IsCompleted() const override
    {
        return IsCompleted_;
    }

    int GetStripeListSliceCount(TCookie cookie) const override
    {
        YT_VERIFY(cookie == ExtractedCookie_);
        YT_VERIFY(ExtractedCookie_ != IChunkPoolOutput::NullCookie);

        int totalSlices = 0;
        for (int poolIndex : xrange(std::ssize(ChunkPools_))) {
            for (auto extractedCookieId : UnderlyingChunkPoolCookies_[poolIndex]) {
                totalSlices += ChunkPools_[poolIndex]->GetStripeListSliceCount(extractedCookieId);
            }
        }

        YT_LOG_TRACE(
            "Retrieved stripe list slice count (Cookie: %v, TotalSlices: %v)",
            cookie,
            totalSlices);

        return totalSlices;
    }

    void Completed(TCookie cookie, const TCompletedJobSummary& jobSummary) override
    {
        YT_LOG_DEBUG(
            "Marking job as completed (Cookie: %v, InterruptionReason: %v)",
            cookie,
            jobSummary.InterruptionReason);

        VerifyExtractedCookieState(cookie, /*shouldBeRunning*/ true, /*shouldBeCompleted*/ false);

        WithUpdateDisabled([&] {
            for (int poolIndex : xrange(std::ssize(ChunkPools_))) {
                const auto& chunkPool = ChunkPools_[poolIndex];
                for (auto extractedCookieId : UnderlyingChunkPoolCookies_[poolIndex]) {
                    chunkPool->Completed(extractedCookieId, jobSummary);
                }
                YT_VERIFY(chunkPool->IsCompleted());

                const auto& jobCounter = chunkPool->GetJobCounter();
                YT_VERIFY(jobCounter->GetTotal() == jobCounter->GetCompletedTotal());

                YT_LOG_DEBUG("Completed pool (PoolIndex: %v)", poolIndex);
            }
        });

        JobCounter_->AddRunning(-1);
        JobCounter_->AddCompleted(1, jobSummary.InterruptionReason);

        auto statistics = ExtractedChunkStripeList_->GetAggregateStatistics();

        DataWeightCounter_->AddRunning(-statistics.DataWeight);
        DataWeightCounter_->AddCompleted(statistics.DataWeight, jobSummary.InterruptionReason);
        RowCounter_->AddRunning(-statistics.RowCount);
        RowCounter_->AddCompleted(statistics.RowCount, jobSummary.InterruptionReason);
        DataSliceCounter_->AddRunning(-ExtractedChunkStripeList_->GetSliceCount());
        DataSliceCounter_->AddCompleted(ExtractedChunkStripeList_->GetSliceCount(), jobSummary.InterruptionReason);

        IsRunning_ = false;
        IsCompleted_ = true;

        UpdateCounters();

        YT_LOG_DEBUG(
            "Job completed (Cookie: %v, DataWeight: %v, RowCount: %v, SliceCount: %v)",
            cookie,
            statistics.DataWeight,
            statistics.RowCount,
            ExtractedChunkStripeList_->GetSliceCount());

        Completed_.Fire();
    }

    void Failed(TCookie cookie) override
    {
        YT_LOG_DEBUG("Marking job as failed (Cookie: %v)", cookie);

        VerifyExtractedCookieState(cookie, /*shouldBeRunning*/ true, /*shouldBeCompleted*/ false);

        WithUpdateDisabled([&] {
            for (int poolIndex : xrange(std::ssize(ChunkPools_))) {
                const auto& chunkPool = ChunkPools_[poolIndex];
                for (auto extractedCookieId : UnderlyingChunkPoolCookies_[poolIndex]) {
                    chunkPool->Failed(extractedCookieId);
                }

                YT_VERIFY(!chunkPool->IsCompleted());

                YT_LOG_DEBUG("Failed pool (PoolIndex: %v)", poolIndex);
            }
        });

        JobCounter_->AddRunning(-1);
        JobCounter_->AddFailed(1);

        auto statistics = ExtractedChunkStripeList_->GetAggregateStatistics();
        i64 sliceCount = ExtractedChunkStripeList_->GetSliceCount();

        DataWeightCounter_->AddRunning(-statistics.DataWeight);
        DataWeightCounter_->AddFailed(statistics.DataWeight);
        RowCounter_->AddRunning(-statistics.RowCount);
        RowCounter_->AddFailed(statistics.RowCount);
        DataSliceCounter_->AddRunning(-sliceCount);
        DataSliceCounter_->AddFailed(sliceCount);

        ResetRunning();

        UpdateCounters();

        YT_LOG_DEBUG(
            "Job failed (Cookie: %v, DataWeight: %v, RowCount: %v, SliceCount: %v)",
            cookie,
            statistics.DataWeight,
            statistics.RowCount,
            sliceCount);
    }

    void Aborted(TCookie cookie, EAbortReason reason) override
    {
        YT_LOG_DEBUG("Aborting job (Cookie: %v, Reason: %v)", cookie, reason);

        VerifyExtractedCookieState(cookie, /*shouldBeRunning*/ true, /*shouldBeCompleted*/ false);

        WithUpdateDisabled([&] {
            for (int poolIndex : xrange(std::ssize(ChunkPools_))) {
                const auto& chunkPool = ChunkPools_[poolIndex];
                for (auto extractedCookieId : UnderlyingChunkPoolCookies_[poolIndex]) {
                    chunkPool->Aborted(extractedCookieId, reason);
                }

                YT_VERIFY(!chunkPool->IsCompleted());

                YT_LOG_DEBUG("Aborted pool (PoolIndex: %v)", poolIndex);
            }
        });

        JobCounter_->AddRunning(-1);
        JobCounter_->AddAborted(1, reason);

        auto statistics = ExtractedChunkStripeList_->GetAggregateStatistics();
        i64 sliceCount = ExtractedChunkStripeList_->GetSliceCount();

        DataWeightCounter_->AddRunning(-statistics.DataWeight);
        DataWeightCounter_->AddAborted(statistics.DataWeight, reason);
        RowCounter_->AddRunning(-statistics.RowCount);
        RowCounter_->AddAborted(statistics.RowCount, reason);
        DataSliceCounter_->AddRunning(-sliceCount);
        DataSliceCounter_->AddAborted(sliceCount, reason);

        ResetRunning();

        UpdateCounters();

        YT_LOG_DEBUG(
            "Job aborted (Cookie: %v, DataWeight: %v, RowCount: %v, SliceCount: %v)",
            cookie,
            statistics.DataWeight,
            statistics.RowCount,
            sliceCount);
    }

    void Lost(TCookie cookie) override
    {
        YT_LOG_DEBUG("Marking job as lost (Cookie: %v)", cookie);

        VerifyExtractedCookieState(cookie, /*shouldBeRunning*/ false, /*shouldBeCompleted*/ true);

        WithUpdateDisabled([&] {
            for (int poolIndex : xrange(std::ssize(ChunkPools_))) {
                const auto& chunkPool = ChunkPools_[poolIndex];
                for (auto extractedCookieId : UnderlyingChunkPoolCookies_[poolIndex]) {
                    chunkPool->Lost(extractedCookieId);
                }

                YT_VERIFY(!chunkPool->IsCompleted());

                YT_LOG_DEBUG("Lost pool (PoolIndex: %v)", poolIndex);
            }
        });

        JobCounter_->AddLost(1);

        auto statistics = ExtractedChunkStripeList_->GetAggregateStatistics();
        i64 sliceCount = ExtractedChunkStripeList_->GetSliceCount();

        DataWeightCounter_->AddLost(statistics.DataWeight);
        RowCounter_->AddLost(statistics.RowCount);
        DataSliceCounter_->AddLost(sliceCount);

        IsCompleted_ = false;
        ExtractedChunkStripeList_.Reset();
        ExtractedCookie_ = IChunkPoolOutput::NullCookie;

        UpdateCounters();

        Uncompleted_.Fire();

        YT_LOG_DEBUG(
            "Job lost (Cookie: %v, DataWeight: %v, RowCount: %v, SliceCount: %v)",
            cookie,
            statistics.DataWeight,
            statistics.RowCount,
            sliceCount);
    }

    bool IsSplittable(TCookie /*cookie*/) const override
    {
        return false;
    }

    void SubscribeChunkTeleported(const TCallback<void(TInputChunkPtr, std::any tag)>&) override
    { }

    void UnsubscribeChunkTeleported(const TCallback<void(TInputChunkPtr, std::any tag)>&) override
    { }

private:
    std::vector<IPersistentChunkPoolOutputPtr> ChunkPools_;
    std::vector<std::vector<TCookie>> UnderlyingChunkPoolCookies_;

    TProgressCounterPtr ParentJobCounter_;
    TProgressCounterPtr ParentDataWeightCounter_;
    TProgressCounterPtr ParentRowCounter_;
    TProgressCounterPtr ParentDataSliceCounter_;

    TCookie ExtractedCookie_ = IChunkPoolOutput::NullCookie;
    TChunkStripeListPtr ExtractedChunkStripeList_;

    bool IsCompleted_ = false;
    bool IsRunning_ = false;

    bool DisableUpdate_ = false;

    void VerifyExtractedCookieState(TCookie cookie, bool shouldBeRunning, bool shouldBeCompleted) const
    {
        YT_VERIFY(cookie == ExtractedCookie_);
        YT_VERIFY(ExtractedCookie_ != IChunkPoolOutput::NullCookie);
        YT_VERIFY(IsCompleted_ == shouldBeCompleted);
        YT_VERIFY(IsRunning_ == shouldBeRunning);
        YT_VERIFY(ExtractedChunkStripeList_);
    }

    void VerifyCanExtract() const
    {
        YT_VERIFY(JobCounter_->GetPending() == 1);
        YT_VERIFY(!IsRunning_);
        YT_VERIFY(!IsCompleted_);
        YT_VERIFY(ExtractedCookie_ == NullCookie);
        YT_VERIFY(!ExtractedChunkStripeList_);
    }

    void ResetRunning()
    {
        IsRunning_ = false;
        ExtractedChunkStripeList_.Reset();
        ExtractedCookie_ = IChunkPoolOutput::NullCookie;
    }

    template <class TFunc>
    void WithUpdateDisabled(TFunc func)
    {
        DisableUpdate_ = true;
        auto guard = Finally([&] { DisableUpdate_ = false; });
        func();
    }

    std::array<TProgressCounter*, 4> GetAllCounters()
    {
        return std::to_array<TProgressCounter*>({
            JobCounter_.Get(),
            DataWeightCounter_.Get(),
            RowCounter_.Get(),
            DataSliceCounter_.Get(),
        });
    }

    void SubscribePoolPendingUpdated(int poolIndex)
    {
        ChunkPools_[poolIndex]->GetJobCounter()->SubscribePendingUpdated(BIND(
            &TChunkPoolsOutputsMerger::UpdateCounters,
            MakeWeak(this)));
    }

    void UpdateCounters()
    {
        // May be disabled when counters may be inconsistent.
        if (DisableUpdate_) {
            YT_LOG_TRACE("Counter update disabled, skipping");
            return;
        }

        YT_LOG_TRACE(
            "Updating counters (IsRunning: %v, IsCompleted: %v, ParentPending: %v, ParentRunning: %v, ParentCompleted: %v)",
            IsRunning_,
            IsCompleted_,
            ParentJobCounter_->GetPending(),
            ParentJobCounter_->GetRunning(),
            ParentJobCounter_->GetCompletedTotal());

        if (IsRunning_) {
            YT_VERIFY(ParentJobCounter_->GetTotal() - ParentJobCounter_->GetCompletedTotal() == ParentJobCounter_->GetRunning());
            for (auto* counter : GetAllCounters()) {
                counter->SetPending(0);
                counter->SetSuspended(0);
            }
            YT_LOG_TRACE("Counters updated in running state");
        } else if (IsCompleted_) {
            YT_VERIFY(ParentJobCounter_->GetTotal() == ParentJobCounter_->GetCompletedTotal());
            for (auto* counter : GetAllCounters()) {
                counter->SetPending(0);
                counter->SetSuspended(0);
            }
            YT_LOG_TRACE("Counters updated in completed state");
        } else if (ParentJobCounter_->GetTotal() - ParentJobCounter_->GetCompletedTotal() == ParentJobCounter_->GetPending()) {
            for (auto* counter : GetAllCounters()) {
                counter->SetSuspended(0);
            }

            JobCounter_->SetPending(ParentJobCounter_->GetPending() == 0 ? 0 : 1);
            DataWeightCounter_->SetPending(ParentDataWeightCounter_->GetTotal() - ParentDataWeightCounter_->GetCompletedTotal());
            RowCounter_->SetPending(ParentRowCounter_->GetTotal() - ParentRowCounter_->GetCompletedTotal());
            DataSliceCounter_->SetPending(ParentDataSliceCounter_->GetTotal() - ParentDataSliceCounter_->GetCompletedTotal());

            YT_LOG_TRACE(
                "Counters updated in pending state (JobPending: %v, DataWeightPending: %v, RowPending: %v, SlicePending: %v)",
                JobCounter_->GetPending(),
                DataWeightCounter_->GetPending(),
                RowCounter_->GetPending(),
                DataSliceCounter_->GetPending());

            if (JobCounter_->GetPending() == 0) {
                IsCompleted_ = true;
                YT_LOG_DEBUG("All jobs completed, marking as completed");
                Completed_.Fire();
            }
        } else {
            JobCounter_->SetSuspended(1);
            DataWeightCounter_->SetSuspended(ParentDataWeightCounter_->GetTotal() - ParentDataWeightCounter_->GetCompletedTotal());
            RowCounter_->SetSuspended(ParentRowCounter_->GetTotal() - ParentRowCounter_->GetCompletedTotal());
            DataSliceCounter_->SetSuspended(ParentDataSliceCounter_->GetTotal() - ParentDataSliceCounter_->GetCompletedTotal());

            for (auto* counter : GetAllCounters()) {
                counter->SetPending(0);
            }

            YT_LOG_TRACE(
                "Counters updated in suspended state (DataWeightSuspended: %v, RowSuspended: %v, SliceSuspended: %v)",
                DataWeightCounter_->GetSuspended(),
                RowCounter_->GetSuspended(),
                DataSliceCounter_->GetSuspended());
        }
    }

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TChunkPoolsOutputsMerger, 0xc40fe250);
};

void TChunkPoolsOutputsMerger::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TChunkPoolOutputWithCountersBase>();
    registrar.template BaseType<TLoggerOwner>();

    PHOENIX_REGISTER_FIELD(1, ChunkPools_);
    PHOENIX_REGISTER_FIELD(2, UnderlyingChunkPoolCookies_);
    PHOENIX_REGISTER_FIELD(3, ParentJobCounter_);
    PHOENIX_REGISTER_FIELD(4, ParentDataWeightCounter_);
    PHOENIX_REGISTER_FIELD(5, ParentRowCounter_);
    PHOENIX_REGISTER_FIELD(6, ParentDataSliceCounter_);
    PHOENIX_REGISTER_FIELD(7, ExtractedCookie_);
    PHOENIX_REGISTER_FIELD(9, ExtractedChunkStripeList_);
    PHOENIX_REGISTER_FIELD(10, IsCompleted_);
    PHOENIX_REGISTER_FIELD(11, IsRunning_);

    registrar.AfterLoad([] (TThis* this_, auto& /*context*/) {
        for (int poolIndex : xrange(std::ssize(this_->ChunkPools_))) {
            this_->SubscribePoolPendingUpdated(poolIndex);
        }
    });
}

PHOENIX_DEFINE_TYPE(TChunkPoolsOutputsMerger);

} // namespace

////////////////////////////////////////////////////////////////////////////////

IPersistentChunkPoolOutputPtr MergeChunkPoolsOutputs(
    std::vector<IPersistentChunkPoolOutputPtr> chunkPools,
    TSerializableLogger logger)
{
    return New<TChunkPoolsOutputsMerger>(std::move(chunkPools), std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
