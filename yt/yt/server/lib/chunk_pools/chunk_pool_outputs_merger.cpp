#include "chunk_pool_outputs_merger.h"

#include "chunk_pool.h"
#include "helpers.h"

#include <yt/yt/server/lib/controller_agent/structs.h>

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
        , JobCounterGuard_(JobCounter_)
        , DataWeightCounterGuard_(DataWeightCounter_)
        , RowCounterGuard_(RowCounter_)
        , DataSliceCounterGuard_(DataSliceCounter_)
    {
        for (int poolIndex : std::views::iota(0, std::ssize(ChunkPools_))) {
            const auto& chunkPool = ChunkPools_[poolIndex];

            chunkPool->GetJobCounter()->AddParent(ParentJobCounter_);
            chunkPool->GetDataWeightCounter()->AddParent(ParentDataWeightCounter_);
            chunkPool->GetRowCounter()->AddParent(ParentRowCounter_);
            chunkPool->GetDataSliceCounter()->AddParent(ParentDataSliceCounter_);

            const auto& jobCounter = chunkPool->GetJobCounter();
            YT_VERIFY(jobCounter->GetRunning() == 0);
            YT_VERIFY(jobCounter->GetCompletedTotal() == 0);
        }

        SubscribeOnUpdates();

        UpdateCounters();

        YT_TLOG_INFO("Chunk pool outputs merger created")
            .With("PoolCount", ChunkPools_.size());
    }

    TChunkStripeStatisticsVector GetApproximateStripeStatistics() const override
    {
        TChunkStripeStatisticsVector result;
        for (const auto& chunkPool : ChunkPools_) {
            auto poolStats = chunkPool->GetApproximateStripeStatistics();
            result.insert(result.end(), poolStats.begin(), poolStats.end());
        }

        YT_TLOG_TRACE("Retrieved approximate stripe statistics")
            .With("TotalStripes", result.size());
        return result;
    }

    TCookie Extract(TNodeId nodeId) override
    {
        YT_TLOG_DEBUG("Extracting job")
            .With("NodeId", nodeId);

        if (JobCounter_->GetPending() == 0) {
            return NullCookie;
        }

        VerifyCanExtract();

        ExtractedCookie_ = static_cast<TCookie>(RandomNumber<ui32>(std::numeric_limits<TCookie>::max()));

        YT_TLOG_DEBUG("Generated cookie")
            .With("Cookie", ExtractedCookie_);

        UnderlyingChunkPoolCookies_.clear();
        UnderlyingChunkPoolCookies_.resize(ChunkPools_.size());

        std::vector<TChunkStripeListPtr> stripeLists;
        stripeLists.reserve(ChunkPools_.size());

        WithUpdateDisabled([&] {
            for (int poolIndex : std::views::iota(0, std::ssize(ChunkPools_))) {
                const auto& chunkPool = ChunkPools_[poolIndex];

                for (auto cookie = chunkPool->Extract(nodeId); cookie != NullCookie; cookie = chunkPool->Extract(nodeId)) {
                    UnderlyingChunkPoolCookies_[poolIndex].push_back(cookie);
                    stripeLists.push_back(chunkPool->GetStripeList(cookie));
                }

                YT_TLOG_DEBUG_IF(!UnderlyingChunkPoolCookies_[poolIndex].empty(), "Extracted cookies from pool")
                    .With("PoolIndex", poolIndex)
                    .With("ExtractedCount", std::ssize(UnderlyingChunkPoolCookies_[poolIndex]));

                const auto& jobCounter = chunkPool->GetJobCounter();
                YT_VERIFY(jobCounter->GetTotal() == std::ssize(UnderlyingChunkPoolCookies_[poolIndex]));
                YT_VERIFY(jobCounter->GetTotal() == jobCounter->GetRunning());
            }
        });

        ExtractedChunkStripeList_ = MergeStripeLists(stripeLists);

        auto statistics = ExtractedChunkStripeList_->GetAggregateStatistics();

        IsRunning_ = true;
        UpdateCounters();

        YT_TLOG_DEBUG("Job extracted")
            .With("Cookie", ExtractedCookie_)
            .With("DataWeight", statistics.DataWeight)
            .With("RowCount", statistics.RowCount)
            .With("SliceCount", ExtractedChunkStripeList_->GetSliceCount());

        return ExtractedCookie_;
    }

    TChunkStripeListPtr GetStripeList(TCookie cookie) override
    {
        YT_TLOG_TRACE("Getting stripe list")
            .With("Cookie", cookie);

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
        for (int poolIndex : std::views::iota(0, std::ssize(ChunkPools_))) {
            for (auto extractedCookieId : UnderlyingChunkPoolCookies_[poolIndex]) {
                totalSlices += ChunkPools_[poolIndex]->GetStripeListSliceCount(extractedCookieId);
            }
        }

        YT_TLOG_TRACE("Retrieved stripe list slice count")
            .With("Cookie", cookie)
            .With("TotalSlices", totalSlices);

        return totalSlices;
    }

    void Completed(TCookie cookie, const TCompletedJobSummary& jobSummary) override
    {
        YT_VERIFY(jobSummary.InterruptionReason == EInterruptionReason::None);

        VerifyExtractedCookieState(cookie, /*shouldBeRunning*/ true, /*shouldBeCompleted*/ false);

        WithUpdateDisabled([&] {
            for (int poolIndex : std::views::iota(0, std::ssize(ChunkPools_))) {
                const auto& chunkPool = ChunkPools_[poolIndex];
                for (auto extractedCookieId : UnderlyingChunkPoolCookies_[poolIndex]) {
                    chunkPool->Completed(extractedCookieId, jobSummary);
                }
                YT_VERIFY(chunkPool->IsCompleted());

                const auto& jobCounter = chunkPool->GetJobCounter();
                YT_VERIFY(jobCounter->GetTotal() == jobCounter->GetCompletedTotal());

                YT_TLOG_DEBUG("Completed pool")
                    .With("PoolIndex", poolIndex);
            }
        });

        ExtractedChunkStripeList_.Reset();

        IsRunning_ = false;
        UpdateCounters();

        YT_TLOG_DEBUG("Job completed")
            .With("Cookie", cookie);
    }

    void Failed(TCookie cookie) override
    {
        VerifyExtractedCookieState(cookie, /*shouldBeRunning*/ true, /*shouldBeCompleted*/ false);

        ApplyAndVerifyNotCompleted(
            [] (const auto& chunkPool, auto extractedCookieId) {
                chunkPool->Failed(extractedCookieId);
            },
            "Failed");

        CallProgressCounterGuards(&TProgressCounterGuard::OnFailed);

        IsRunning_ = false;
        UpdateCounters();

        ExtractedChunkStripeList_.Reset();
        ExtractedCookie_ = IChunkPoolOutput::NullCookie;

        YT_TLOG_DEBUG("Job failed")
            .With("Cookie", cookie);
    }

    void Aborted(TCookie cookie, EAbortReason reason) override
    {
        VerifyExtractedCookieState(cookie, /*shouldBeRunning*/ true, /*shouldBeCompleted*/ false);

        ApplyAndVerifyNotCompleted(
            [reason] (const auto& chunkPool, auto extractedCookieId) {
                chunkPool->Aborted(extractedCookieId, reason);
            },
            "Aborted");

        CallProgressCounterGuards(&TProgressCounterGuard::OnAborted, reason);

        IsRunning_ = false;
        UpdateCounters();

        ExtractedChunkStripeList_.Reset();
        ExtractedCookie_ = IChunkPoolOutput::NullCookie;

        YT_TLOG_DEBUG("Job aborted")
            .With("Cookie", cookie);
    }

    void Lost(TCookie cookie) override
    {
        VerifyExtractedCookieState(cookie, /*shouldBeRunning*/ false, /*shouldBeCompleted*/ true);

        ApplyAndVerifyNotCompleted(
            [] (const auto& chunkPool, auto extractedCookieId) {
                chunkPool->Lost(extractedCookieId);
            },
            "Lost");

        CallProgressCounterGuards(&TProgressCounterGuard::OnLost);

        ExtractedCookie_ = IChunkPoolOutput::NullCookie;

        UpdateCounters();

        YT_TLOG_DEBUG("Job lost")
            .With("Cookie", cookie);
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

    TProgressCounterGuard JobCounterGuard_;
    TProgressCounterGuard DataWeightCounterGuard_;
    TProgressCounterGuard RowCounterGuard_;
    TProgressCounterGuard DataSliceCounterGuard_;

    TCookie ExtractedCookie_ = IChunkPoolOutput::NullCookie;
    TChunkStripeListPtr ExtractedChunkStripeList_;

    bool IsCompleted_ = false;
    bool IsRunning_ = false;

    bool DisableUpdate_ = false;

    template <class... TArgs>
    void CallProgressCounterGuards(void (TProgressCounterGuard::*Method)(TArgs...), const TArgs&... args)
    {
        (JobCounterGuard_.*Method)(args...);
        (DataWeightCounterGuard_.*Method)(args...);
        (RowCounterGuard_.*Method)(args...);
        (DataSliceCounterGuard_.*Method)(args...);
    }

    void VerifyExtractedCookieState(TCookie cookie, bool shouldBeRunning, bool shouldBeCompleted) const
    {
        YT_VERIFY(cookie == ExtractedCookie_);
        YT_VERIFY(ExtractedCookie_ != IChunkPoolOutput::NullCookie);
        YT_VERIFY(IsCompleted_ == shouldBeCompleted);
        YT_VERIFY(IsRunning_ == shouldBeRunning);
        if (shouldBeCompleted) {
            YT_VERIFY(!ExtractedChunkStripeList_);
        } else {
            YT_VERIFY(ExtractedChunkStripeList_);
        }
    }

    void VerifyCanExtract() const
    {
        YT_VERIFY(JobCounter_->GetPending() == 1);
        YT_VERIFY(!IsRunning_);
        YT_VERIFY(!IsCompleted_);
        YT_VERIFY(ExtractedCookie_ == NullCookie);
        YT_VERIFY(!ExtractedChunkStripeList_);
    }

    template <class TFunc>
    void WithUpdateDisabled(TFunc&& func)
    {
        DisableUpdate_ = true;
        auto guard = Finally([&] { DisableUpdate_ = false; });
        func();
    }

    template <class TAction>
    void ApplyAndVerifyNotCompleted(TAction&& action, TStringBuf actionName)
    {
        WithUpdateDisabled([&] {
            for (int poolIndex : std::views::iota(0, std::ssize(ChunkPools_))) {
                if (UnderlyingChunkPoolCookies_[poolIndex].empty()) {
                    continue;
                }

                const auto& chunkPool = ChunkPools_[poolIndex];
                for (auto extractedCookieId : UnderlyingChunkPoolCookies_[poolIndex]) {
                    action(chunkPool, extractedCookieId);
                }

                YT_VERIFY(!chunkPool->IsCompleted());

                YT_TLOG_DEBUG("Applied action to pool")
                    .With("Action", actionName)
                    .With("PoolIndex", poolIndex);
            }
        });
    }

    auto GetAllParentCounters() const
    {
        return std::to_array<TProgressCounter*>({
            ParentJobCounter_.Get(),
            ParentDataWeightCounter_.Get(),
            ParentRowCounter_.Get(),
            ParentDataSliceCounter_.Get(),
        });
    }

    void SubscribeOnUpdates()
    {
        ParentJobCounter_->SubscribePendingUpdated(BIND(
            &TChunkPoolsOutputsMerger::UpdateCounters,
            MakeWeak(this)));

        for (const auto& chunkPool : ChunkPools_) {
            chunkPool->SubscribeCompleted(BIND(
                &TChunkPoolsOutputsMerger::CheckCompleted,
                MakeWeak(this)));
            chunkPool->SubscribeUncompleted(BIND(
                &TChunkPoolsOutputsMerger::CheckCompleted,
                MakeWeak(this)));
        }
    }

    void CheckCompleted()
    {
        bool wasCompleted = IsCompleted_;
        IsCompleted_ = std::ranges::all_of(ChunkPools_, [] (const auto& chunkPool) { return chunkPool->IsCompleted(); });

        if (!wasCompleted && IsCompleted_) {
            YT_TLOG_DEBUG("All pools completed, firing completed callback");
            Completed_.Fire();
        } else if (wasCompleted && !IsCompleted_) {
            YT_TLOG_DEBUG("Some pools become uncompleted, firing uncompleted callback");
            Uncompleted_.Fire();
        }
    }

    void UpdateCounters()
    {
        // May be disabled when counters may be inconsistent.
        if (DisableUpdate_) {
            YT_TLOG_TRACE("Counter update disabled, skipping");
            return;
        }

        YT_TLOG_TRACE("Updating counters")
            .With("IsRunning", IsRunning_)
            .With("IsCompleted", IsCompleted_)
            .With("ParentJobCounter", ParentJobCounter_);

        auto verifyCompletedZero = [&] {
            for (auto* counter : GetAllParentCounters()) {
                YT_VERIFY(counter->GetCompletedTotal() == 0);
            }
        };

        if (IsRunning_) {
            YT_VERIFY(ParentJobCounter_->GetTotal() == ParentJobCounter_->GetRunning());
            verifyCompletedZero();

            auto statistics = ExtractedChunkStripeList_->GetAggregateStatistics();

            JobCounterGuard_.SetValue(1);
            DataWeightCounterGuard_.SetValue(statistics.DataWeight);
            RowCounterGuard_.SetValue(statistics.RowCount);
            DataSliceCounterGuard_.SetValue(ExtractedChunkStripeList_->GetSliceCount());

            CallProgressCounterGuards(&TProgressCounterGuard::SetCategory, EProgressCategory::Running);


            YT_TLOG_TRACE("Counters updated in running state");
        } else if (IsCompleted_) {
            YT_VERIFY(ParentJobCounter_->GetTotal() == ParentJobCounter_->GetCompletedTotal());

            CallProgressCounterGuards(&TProgressCounterGuard::SetCategory, EProgressCategory::Completed);

            YT_TLOG_TRACE("Counters updated in completed state");
        } else if (ParentJobCounter_->GetTotal() == ParentJobCounter_->GetPending()) {
            verifyCompletedZero();

            JobCounterGuard_.SetValue(ParentJobCounter_->GetPending() == 0 ? 0 : 1);
            DataWeightCounterGuard_.SetValue(ParentDataWeightCounter_->GetTotal());
            RowCounterGuard_.SetValue(ParentRowCounter_->GetTotal());
            DataSliceCounterGuard_.SetValue(ParentDataSliceCounter_->GetTotal());

            CallProgressCounterGuards(&TProgressCounterGuard::SetCategory, EProgressCategory::Pending);

            YT_TLOG_TRACE("Counters updated in pending state")
                .With("JobPending", JobCounter_->GetPending())
                .With("DataWeightPending", DataWeightCounter_->GetPending())
                .With("RowPending", RowCounter_->GetPending())
                .With("SlicePending", DataSliceCounter_->GetPending());
        } else {
            verifyCompletedZero();

            JobCounterGuard_.SetValue(ParentJobCounter_->GetTotal() > 0 ? 1 : 0);
            DataWeightCounterGuard_.SetValue(ParentDataWeightCounter_->GetTotal());
            RowCounterGuard_.SetValue(ParentRowCounter_->GetTotal());
            DataSliceCounterGuard_.SetValue(ParentDataSliceCounter_->GetTotal());

            CallProgressCounterGuards(&TProgressCounterGuard::SetCategory, EProgressCategory::Suspended);

            YT_TLOG_TRACE("Counters updated in suspended state")
                .With("DataWeightSuspended", DataWeightCounter_->GetSuspended())
                .With("RowSuspended", RowCounter_->GetSuspended())
                .With("SliceSuspended", DataSliceCounter_->GetSuspended());
        }

        CheckCompleted();

        YT_VERIFY(JobCounter_->GetTotal() <= 1);
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
    PHOENIX_REGISTER_FIELD(7, JobCounterGuard_);
    PHOENIX_REGISTER_FIELD(8, DataWeightCounterGuard_);
    PHOENIX_REGISTER_FIELD(9, RowCounterGuard_);
    PHOENIX_REGISTER_FIELD(10, DataSliceCounterGuard_);
    PHOENIX_REGISTER_FIELD(11, ExtractedCookie_);
    PHOENIX_REGISTER_FIELD(12, ExtractedChunkStripeList_);
    PHOENIX_REGISTER_FIELD(13, IsCompleted_);
    PHOENIX_REGISTER_FIELD(14, IsRunning_);

    registrar.AfterLoad([] (TThis* this_, auto& /*context*/) {
        this_->SubscribeOnUpdates();
    });
}

PHOENIX_DEFINE_TYPE(TChunkPoolsOutputsMerger);

} // namespace

////////////////////////////////////////////////////////////////////////////////

IPersistentChunkPoolOutputPtr MergeChunkPoolOutputs(
    std::vector<IPersistentChunkPoolOutputPtr> chunkPools,
    TSerializableLogger logger)
{
    return New<TChunkPoolsOutputsMerger>(std::move(chunkPools), std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
