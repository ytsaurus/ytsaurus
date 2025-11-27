#include "chunk_pool_output_merger.h"

#include "chunk_pool.h"
#include "helpers.h"

#include <yt/yt/server/lib/controller_agent/structs.h>

#include <util/generic/xrange.h>

#include <util/random/random.h>

namespace NYT::NChunkPools {

using namespace NChunkClient;
using namespace NControllerAgent;
using namespace NNodeTrackerClient;
using namespace NScheduler;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TChunkPoolsOutputsMerger
    : public TChunkPoolOutputWithCountersBase
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(), Completed);
    DEFINE_SIGNAL_OVERRIDE(void(), Uncompleted);

public:
    //! Used only for persistence.
    TChunkPoolsOutputsMerger() = default;

    TChunkPoolsOutputsMerger(std::vector<IPersistentChunkPoolOutputPtr> chunkPools)
        : ChunkPools_(std::move(chunkPools))
        , ParentJobCounter_(New<TProgressCounter>())
        , ParentDataWeightCounter_(New<TProgressCounter>())
        , ParentRowCounter_(New<TProgressCounter>())
        , ParentDataSliceCounter_(New<TProgressCounter>())
    {
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
        }

        UpdateCounters();
    }

    TChunkStripeStatisticsVector GetApproximateStripeStatistics() const override
    {
        TChunkStripeStatisticsVector result;
        for (const auto& chunkPool : ChunkPools_) {
            auto poolStats = chunkPool->GetApproximateStripeStatistics();
            result.insert(result.end(), poolStats.begin(), poolStats.end());
        }
        return result;
    }

    TCookie Extract(TNodeId nodeId) override
    {
        YT_VERIFY(JobCounter_->GetPending() == 1);
        YT_VERIFY(!IsRunning_);
        YT_VERIFY(!IsCompleted_);
        YT_VERIFY(ExtractedCookie_ == NullCookie);
        YT_VERIFY(!ExtractedChunkStripeList_);

        ExtractedCookie_ = static_cast<TCookie>(RandomNumber<ui32>(std::numeric_limits<TCookie>::max()));

        UnderlyingChunkPoolCookies_.clear();
        UnderlyingChunkPoolCookies_.resize(ChunkPools_.size());

        std::vector<TChunkStripeListPtr> stripeLists;
        stripeLists.reserve(ChunkPools_.size());

        {
            DisableUpdate_ = true;
            auto finallyGuard = Finally([&] { DisableUpdate_ = false; });

            for (int poolIndex : xrange(std::ssize(ChunkPools_))) {
                const auto& chunkPool = ChunkPools_[poolIndex];

                for (auto cookie = chunkPool->Extract(nodeId); cookie != NullCookie; cookie = chunkPool->Extract(nodeId)) {
                    UnderlyingChunkPoolCookies_[poolIndex].push_back(cookie);
                    stripeLists.push_back(chunkPool->GetStripeList(cookie));
                }

                const auto& jobCounter = chunkPool->GetJobCounter();
                YT_VERIFY(jobCounter->GetTotal() == std::ssize(UnderlyingChunkPoolCookies_[poolIndex]));
                YT_VERIFY(jobCounter->GetTotal() == jobCounter->GetRunning());
            }
        }

        ExtractedChunkStripeList_ = MergeStripeLists(stripeLists);

        IsRunning_ = true;

        JobCounter_->AddRunning(1);

        const auto& statistics = ExtractedChunkStripeList_->GetAggregateStatistics();

        DataWeightCounter_->AddRunning(statistics.DataWeight);
        RowCounter_->AddRunning(statistics.RowCount);
        DataSliceCounter_->AddRunning(ExtractedChunkStripeList_->GetSliceCount());

        UpdateCounters();

        return ExtractedCookie_;
    }

    TChunkStripeListPtr GetStripeList(TCookie cookie) override
    {
        YT_VERIFY(cookie == ExtractedCookie_);
        YT_VERIFY(ExtractedCookie_ != IChunkPoolOutput::NullCookie);
        YT_VERIFY(std::ssize(UnderlyingChunkPoolCookies_) == std::ssize(ChunkPools_));
        YT_VERIFY(!IsCompleted_);
        YT_VERIFY(IsRunning_);
        YT_VERIFY(ExtractedChunkStripeList_);

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
        return totalSlices;
    }

    void Completed(TCookie cookie, const TCompletedJobSummary& jobSummary) override
    {
        YT_VERIFY(cookie == ExtractedCookie_);
        YT_VERIFY(ExtractedCookie_ != IChunkPoolOutput::NullCookie);
        YT_VERIFY(!IsCompleted_);
        YT_VERIFY(IsRunning_);
        YT_VERIFY(ExtractedChunkStripeList_);

        {
            DisableUpdate_ = true;
            auto finallyGuard = Finally([&] { DisableUpdate_ = false; });

            for (int poolIndex : xrange(std::ssize(ChunkPools_))) {
                const auto& chunkPool = ChunkPools_[poolIndex];
                for (auto extractedCookieId : UnderlyingChunkPoolCookies_[poolIndex]) {
                    chunkPool->Completed(extractedCookieId, jobSummary);
                }
                YT_VERIFY(chunkPool->IsCompleted());

                const auto& jobCounter = chunkPool->GetJobCounter();
                YT_VERIFY(jobCounter->GetTotal() == jobCounter->GetCompletedTotal());
            }
        }

        JobCounter_->AddRunning(-1);
        JobCounter_->AddCompleted(1, jobSummary.InterruptionReason);

        const auto& statistics = ExtractedChunkStripeList_->GetAggregateStatistics();

        DataWeightCounter_->AddRunning(-statistics.DataWeight);
        DataWeightCounter_->AddCompleted(statistics.DataWeight, jobSummary.InterruptionReason);
        RowCounter_->AddRunning(-statistics.RowCount);
        RowCounter_->AddCompleted(statistics.RowCount, jobSummary.InterruptionReason);
        DataSliceCounter_->AddRunning(-ExtractedChunkStripeList_->GetSliceCount());
        DataSliceCounter_->AddCompleted(ExtractedChunkStripeList_->GetSliceCount(), jobSummary.InterruptionReason);

        IsRunning_ = false;
        IsCompleted_ = true;

        UpdateCounters();

        Completed_.Fire();
    }

    void Failed(TCookie cookie) override
    {
        YT_VERIFY(cookie == ExtractedCookie_);
        YT_VERIFY(ExtractedCookie_ != IChunkPoolOutput::NullCookie);
        YT_VERIFY(!IsCompleted_);
        YT_VERIFY(IsRunning_);
        YT_VERIFY(ExtractedChunkStripeList_);

        {
            DisableUpdate_ = true;
            auto finallyGuard = Finally([&] { DisableUpdate_ = false; });

            for (int poolIndex : xrange(std::ssize(ChunkPools_))) {
                const auto& chunkPool = ChunkPools_[poolIndex];
                for (auto extractedCookieId : UnderlyingChunkPoolCookies_[poolIndex]) {
                    chunkPool->Failed(extractedCookieId);
                }

                YT_VERIFY(!chunkPool->IsCompleted());
            }
        }

        JobCounter_->AddRunning(-1);
        JobCounter_->AddFailed(1);

        const auto& statistics = ExtractedChunkStripeList_->GetAggregateStatistics();

        DataWeightCounter_->AddRunning(-statistics.DataWeight);
        DataWeightCounter_->AddFailed(statistics.DataWeight);
        RowCounter_->AddRunning(-statistics.RowCount);
        RowCounter_->AddFailed(statistics.RowCount);
        DataSliceCounter_->AddRunning(-ExtractedChunkStripeList_->GetSliceCount());
        DataSliceCounter_->AddFailed(ExtractedChunkStripeList_->GetSliceCount());

        IsRunning_ = false;
        ExtractedChunkStripeList_.Reset();
        ExtractedCookie_ = IChunkPoolOutput::NullCookie;

        UpdateCounters();
    }

    void Aborted(TCookie cookie, EAbortReason reason) override
    {
        YT_VERIFY(cookie == ExtractedCookie_);
        YT_VERIFY(ExtractedCookie_ != IChunkPoolOutput::NullCookie);
        YT_VERIFY(!IsCompleted_);
        YT_VERIFY(IsRunning_);
        YT_VERIFY(ExtractedChunkStripeList_);

        {
            DisableUpdate_ = true;
            auto finallyGuard = Finally([&] { DisableUpdate_ = false; });

            for (int poolIndex : xrange(std::ssize(ChunkPools_))) {
                const auto& chunkPool = ChunkPools_[poolIndex];
                for (auto extractedCookieId : UnderlyingChunkPoolCookies_[poolIndex]) {
                    chunkPool->Aborted(extractedCookieId, reason);
                }

                YT_VERIFY(!chunkPool->IsCompleted());
            }
        }

        JobCounter_->AddRunning(-1);
        JobCounter_->AddAborted(1, reason);

        const auto& statistics = ExtractedChunkStripeList_->GetAggregateStatistics();

        DataWeightCounter_->AddRunning(-statistics.DataWeight);
        DataWeightCounter_->AddAborted(statistics.DataWeight, reason);
        RowCounter_->AddRunning(-statistics.RowCount);
        RowCounter_->AddAborted(statistics.RowCount, reason);
        DataSliceCounter_->AddRunning(-ExtractedChunkStripeList_->GetSliceCount());
        DataSliceCounter_->AddAborted(ExtractedChunkStripeList_->GetSliceCount(), reason);

        IsRunning_ = false;
        ExtractedChunkStripeList_.Reset();
        ExtractedCookie_ = IChunkPoolOutput::NullCookie;

        UpdateCounters();
    }

    void Lost(TCookie cookie) override
    {
        YT_VERIFY(cookie == ExtractedCookie_);
        YT_VERIFY(ExtractedCookie_ != IChunkPoolOutput::NullCookie);
        YT_VERIFY(IsCompleted_);
        YT_VERIFY(!IsRunning_);
        YT_VERIFY(ExtractedChunkStripeList_);

        {
            DisableUpdate_ = true;
            auto finallyGuard = Finally([&] { DisableUpdate_ = false; });

            for (int poolIndex : xrange(std::ssize(ChunkPools_))) {
                const auto& chunkPool = ChunkPools_[poolIndex];
                for (auto extractedCookieId : UnderlyingChunkPoolCookies_[poolIndex]) {
                    chunkPool->Lost(extractedCookieId);
                }

                YT_VERIFY(!chunkPool->IsCompleted());
            }
        }

        JobCounter_->AddLost(1);

        const auto& statistics = ExtractedChunkStripeList_->GetAggregateStatistics();

        DataWeightCounter_->AddLost(statistics.DataWeight);
        RowCounter_->AddLost(statistics.RowCount);
        DataSliceCounter_->AddLost(ExtractedChunkStripeList_->GetSliceCount());

        IsCompleted_ = false;
        ExtractedChunkStripeList_.Reset();
        ExtractedCookie_ = IChunkPoolOutput::NullCookie;

        UpdateCounters();

        Uncompleted_.Fire();
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
            return;
        }

        if (IsRunning_) {
            YT_VERIFY(ParentJobCounter_->GetTotal() - ParentJobCounter_->GetCompletedTotal() == ParentJobCounter_->GetRunning());
            for (auto* counter : GetAllCounters()) {
                counter->SetPending(0);
                counter->SetSuspended(0);
            }
        } else if (IsCompleted_) {
            YT_VERIFY(ParentJobCounter_->GetTotal() == ParentJobCounter_->GetCompletedTotal());
            for (auto* counter : GetAllCounters()) {
                counter->SetPending(0);
                counter->SetSuspended(0);
            }
        } else if (ParentJobCounter_->GetTotal() - ParentJobCounter_->GetCompletedTotal() == ParentJobCounter_->GetPending()) {
            for (auto* counter : GetAllCounters()) {
                counter->SetSuspended(0);
            }

            JobCounter_->SetPending(ParentJobCounter_->GetPending() == 0 ? 0 : 1);
            DataWeightCounter_->SetPending(ParentDataWeightCounter_->GetTotal() - ParentDataWeightCounter_->GetCompletedTotal());
            RowCounter_->SetPending(ParentRowCounter_->GetTotal() - ParentRowCounter_->GetCompletedTotal());
            DataSliceCounter_->SetPending(ParentDataSliceCounter_->GetTotal() - ParentDataSliceCounter_->GetCompletedTotal());

            if (JobCounter_->GetPending() == 0) {
                IsCompleted_ = true;
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
        }
    }

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TChunkPoolsOutputsMerger, 0xc40fe250);
};

void TChunkPoolsOutputsMerger::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TChunkPoolOutputWithCountersBase>();

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

IPersistentChunkPoolOutputPtr MergeChunkPoolsOutputs(std::vector<IPersistentChunkPoolOutputPtr> chunkPools)
{
    return New<TChunkPoolsOutputsMerger>(std::move(chunkPools));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
