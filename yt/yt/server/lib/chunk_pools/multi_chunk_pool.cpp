#include "multi_chunk_pool.h"
#include "chunk_pool.h"
#include "chunk_stripe.h"

#include <yt/server/lib/controller_agent/progress_counter.h>

#include <util/generic/noncopyable.h>

namespace NYT::NChunkPools {

using namespace NChunkClient;
using namespace NControllerAgent;
using namespace NNodeTrackerClient;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkPoolInput
    : public virtual IChunkPoolInput
{
public:
    using TExternalCookie = TCookie;

    //! Pair of underlying pool index and its cookie uniquely representing cookie in multi chunk pool.
    using TCookieDescriptor = std::pair<int, TCookie>;

    //! Used only for persistence.
    TMultiChunkPoolInput() = default;

    explicit TMultiChunkPoolInput(std::vector<IChunkPoolInput*> underlyingPools)
        : UnderlyingPools_(std::move(underlyingPools))
    { }

    virtual TExternalCookie Add(TChunkStripePtr stripe) override
    {
        YT_VERIFY(stripe->PartitionTag);
        auto poolIndex = *stripe->PartitionTag;
        auto* pool = Pool(poolIndex);
        auto guard = MakeGuard(poolIndex);
        auto cookie = pool->Add(std::move(stripe));

        return AddCookie(poolIndex, cookie);
    }

    virtual TExternalCookie AddWithKey(TChunkStripePtr stripe, TChunkStripeKey key)
    {
        YT_VERIFY(stripe->PartitionTag);
        auto poolIndex = *stripe->PartitionTag;
        auto* pool = Pool(poolIndex);
        auto guard = MakeGuard(poolIndex);
        auto cookie = pool->AddWithKey(std::move(stripe), key);

        return AddCookie(poolIndex, cookie);
    }

    virtual void Suspend(TExternalCookie externalCookie) override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        auto* pool = Pool(poolIndex);
        auto guard = MakeGuard(poolIndex);
        pool->Suspend(cookie);
    }

    virtual void Resume(TExternalCookie externalCookie) override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        auto* pool = Pool(poolIndex);
        auto guard = MakeGuard(poolIndex);
        pool->Resume(cookie);
    }

    virtual void Reset(
        TExternalCookie externalCookie,
        TChunkStripePtr stripe,
        TInputChunkMappingPtr mapping) override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        stripe->PartitionTag = poolIndex;
        auto* cookiePool = Pool(poolIndex);
        auto guard = MakeGuard(poolIndex);

        cookiePool->Reset(cookie, std::move(stripe), std::move(mapping));
    }

    virtual void Finish() override
    {
        for (int poolIndex = 0; poolIndex < UnderlyingPools_.size(); ++poolIndex) {
            auto* pool = Pool(poolIndex);
            auto guard = MakeGuard(poolIndex);
            pool->Finish();
        }
        IsFinished_ = true;
    }

    virtual bool IsFinished() const override
    {
        return IsFinished_;
    }

    virtual void Persist(const TPersistenceContext& context) override
    {
        using ::NYT::Persist;

        Persist(context, UnderlyingPools_);
        Persist<TVectorSerializer<TTupleSerializer<std::pair<int, TCookie>, 2>>>(context, Cookies_);
        Persist(context, IsFinished_);
    }

protected:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMultiChunkPoolInput, 0xe712ad5b);

    std::vector<IChunkPoolInput*> UnderlyingPools_;

    //! Mapping external_cookie -> cookie_descriptor.
    std::vector<TCookieDescriptor> Cookies_;

    bool IsFinished_ = false;

    TCookieDescriptor Cookie(TExternalCookie externalCookie) const
    {
        YT_VERIFY(externalCookie >= 0);
        YT_VERIFY(externalCookie < Cookies_.size());

        return Cookies_[externalCookie];
    }

    TExternalCookie AddCookie(int poolIndex, TCookie cookie)
    {
        auto externalCookie = Cookies_.size();
        Cookies_.emplace_back(poolIndex, cookie);

        return externalCookie;
    }

    IChunkPoolInput* Pool(int poolIndex)
    {
        YT_VERIFY(poolIndex >= 0);
        YT_VERIFY(poolIndex < UnderlyingPools_.size());

        return UnderlyingPools_[poolIndex];
    }

    //! It's required to create such a guard before any mutating actions with underlying pools
    //! to have cumulative statistics consistent with underlying pools' statistics.
    virtual std::any MakeGuard(int /*poolIndex*/)
    {
        return std::any{};
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TMultiChunkPoolInput);

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkPoolOutput
    : public virtual IChunkPoolOutput
{
public:
    using TExternalCookie = TCookie;

    //! Pair of underlying pool and its cookie uniquely representing cookie in multi chunk pool.
    using TCookieDescriptor = std::pair<int, TCookie>;

    //! Used only for persistence.
    TMultiChunkPoolOutput() = default;

    explicit TMultiChunkPoolOutput(std::vector<IChunkPoolOutput*> underlyingPools)
        : UnderlyingPools_(std::move(underlyingPools))
        , PendingPoolIterators_(UnderlyingPools_.size(), PendingPools_.end())
    {
        // AddPoolStatistics adds pool to the beginning of the pending pool queue, so we add pools here in reverse
        // order to make order in queue more natural.
        for (int poolIndex = static_cast<int>(UnderlyingPools_.size()) - 1; poolIndex >= 0; --poolIndex) {
            auto* pool = Pool(poolIndex);

            // Chunk pools with output order are not supported.
            YT_VERIFY(!pool->GetOutputOrder());

            if (const auto& jobCounter = pool->GetJobCounter()) {
                jobCounter->SetParent(JobCounter_);
            }

            pool->SubscribeChunkTeleported(
                BIND([this, poolIndex] (TInputChunkPtr teleportChunk, std::any /*tag*/) {
                    ChunkTeleported_.Fire(std::move(teleportChunk), poolIndex);
                }));
            AddPoolStatistics(poolIndex, +1);
        }
    }

    virtual i64 GetTotalDataWeight() const override
    {
        return TotalDataWeight_;
    }

    virtual i64 GetRunningDataWeight() const override
    {
        return RunningDataWeight_;
    }

    virtual i64 GetCompletedDataWeight() const override
    {
        return CompletedDataWeight_;
    }

    virtual i64 GetPendingDataWeight() const override
    {
        return PendingDataWeight_;
    }

    virtual i64 GetTotalRowCount() const override
    {
        return TotalRowCount_;
    }

    const TProgressCounterPtr& GetJobCounter() const override
    {
        return JobCounter_;
    }

    virtual i64 GetDataSliceCount() const override
    {
        return DataSliceCount_;
    }

    virtual TOutputOrderPtr GetOutputOrder() const override
    {
        // Not supported for now.
        return nullptr;
    }

    virtual i64 GetLocality(TNodeId nodeId) const override
    {
        return 0;
    }

    virtual TChunkStripeStatisticsVector GetApproximateStripeStatistics() const override
    {
        if (PendingPools_.empty()) {
            return {};
        }

        return CurrentPool()->GetApproximateStripeStatistics();
    }

    virtual TExternalCookie Extract(TNodeId nodeId) override
    {
        if (PendingPools_.empty()) {
            return NullCookie;
        }

        auto poolIndex = CurrentPoolIndex();
        auto guard = MakeGuard(poolIndex);
        auto cookie = Pool(poolIndex)->Extract();
        YT_VERIFY(cookie != NullCookie);

        auto externalCookie = Cookies_.size();
        Cookies_.emplace_back(poolIndex, cookie);

        return externalCookie;
    }

    virtual TChunkStripeListPtr GetStripeList(TExternalCookie externalCookie) override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        auto poolGuard = MakeGuard(poolIndex);
        auto stripeList = Pool(poolIndex)->GetStripeList(cookie);
        stripeList->PartitionTag = poolIndex;

        return stripeList;
    }

    virtual bool IsCompleted() const override
    {
        return ActivePoolCount_ == 0;
    }

    virtual int GetTotalJobCount() const override
    {
        return TotalJobCount_;
    }

    virtual int GetPendingJobCount() const override
    {
        return PendingJobCount_;
    }

    virtual int GetStripeListSliceCount(TExternalCookie externalCookie) const override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        return Pool(poolIndex)->GetStripeListSliceCount(cookie);
    }

    virtual void Completed(TExternalCookie externalCookie, const TCompletedJobSummary& jobSummary) override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        auto guard = MakeGuard(poolIndex);
        Pool(poolIndex)->Completed(cookie, jobSummary);
    }

    virtual void Failed(TExternalCookie externalCookie) override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        auto guard = MakeGuard(poolIndex);
        Pool(poolIndex)->Failed(cookie);
    }

    virtual void Aborted(TExternalCookie externalCookie, EAbortReason reason) override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        auto guard = MakeGuard(poolIndex);
        Pool(poolIndex)->Aborted(cookie, reason);
    }
    
    virtual void Lost(TExternalCookie externalCookie) override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        auto guard = MakeGuard(poolIndex);
        Pool(poolIndex)->Lost(cookie);
    }

    virtual void Persist(const TPersistenceContext& context) override
    {
        using ::NYT::Persist;

        Persist(context, UnderlyingPools_);
        Persist(context, JobCounter_);
        Persist<TVectorSerializer<TTupleSerializer<std::pair<int, TCookie>, 2>>>(context, Cookies_);

        if (context.IsLoad()) {
            // NB(gritukan): It seems hard to persist list iterators, so we do not persist statistics
            // and restore them from scratch here using underlying pools statistics.
            PendingPoolIterators_ = std::vector<std::list<int>::iterator>(UnderlyingPools_.size(), PendingPools_.end());
            for (int poolIndex = static_cast<int>(UnderlyingPools_.size()) - 1; poolIndex >= 0; --poolIndex) {
                auto* pool = Pool(poolIndex);
                AddPoolStatistics(poolIndex, +1);

                pool->SubscribeChunkTeleported(
                    BIND([this, poolIndex] (TInputChunkPtr teleportChunk, std::any /*tag*/) {
                        ChunkTeleported_.Fire(std::move(teleportChunk), poolIndex);
                    }));
            }
        }
    }

protected:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMultiChunkPoolOutput, 0x135ffa20);

    std::vector<IChunkPoolOutput*> UnderlyingPools_;

    //! Sum of total data weight over all underlying pools.
    i64 TotalDataWeight_ = 0;

    //! Sum of running data weight over all underlying pools.
    i64 RunningDataWeight_ = 0;

    //! Sum of completed data weight over all underlying pools.
    i64 CompletedDataWeight_ = 0;

    //! Sum of pending data weight over all underlying pools.
    i64 PendingDataWeight_ = 0;

    //! Sum of total row count over all underlying pools.
    i64 TotalRowCount_ = 0;

    //! Parent of all underlying pool job counters.
    TProgressCounterPtr JobCounter_ = New<TProgressCounter>(0);

    //! Sum of data slice count over all underlying pools.
    i64 DataSliceCount_ = 0;

    //! Sum of total job count over all underlying pools.
    int TotalJobCount_ = 0;

    //! Sum of pending job count over all underlying pools.
    int PendingJobCount_ = 0;

    //! Number of active (incomplete) chunk pools.
    int ActivePoolCount_ = 0;

    std::vector<TCookieDescriptor> Cookies_;

    //! Queue of pools with pending jobs.
    std::list<int> PendingPools_;

    //! Mapping pool_index -> iterator in pending_pools for pools with pending jobs.
    std::vector<std::list<int>::iterator> PendingPoolIterators_;

    IChunkPoolOutput* Pool(int poolIndex)
    {
        YT_VERIFY(poolIndex >= 0);
        YT_VERIFY(poolIndex < UnderlyingPools_.size());

        return UnderlyingPools_[poolIndex];
    }

    const IChunkPoolOutput* Pool(int poolIndex) const
    {
        YT_VERIFY(poolIndex >= 0);
        YT_VERIFY(poolIndex < UnderlyingPools_.size());

        return UnderlyingPools_[poolIndex];
    }

    int CurrentPoolIndex() const
    {
        YT_VERIFY(!PendingPools_.empty());

        return *PendingPools_.begin();
    }

    const IChunkPoolOutput* CurrentPool() const
    {
        return Pool(CurrentPoolIndex());
    }
    
    TCookieDescriptor Cookie(TExternalCookie externalCookie) const
    {
        return Cookies_[externalCookie];
    }

    //! Adds `pool' statistics multiplied by `multiplier' to cumulative pool statistics.
    void AddPoolStatistics(int poolIndex, int multiplier)
    {
        auto* pool = Pool(poolIndex);

        TotalDataWeight_ += pool->GetTotalDataWeight() * multiplier;
        RunningDataWeight_ += pool->GetRunningDataWeight() * multiplier;
        CompletedDataWeight_ += pool->GetCompletedDataWeight() * multiplier;
        PendingDataWeight_ += pool->GetPendingDataWeight() * multiplier;
        TotalRowCount_ += pool->GetTotalRowCount() * multiplier;
        DataSliceCount_ += pool->GetDataSliceCount() * multiplier;
        TotalJobCount_ += pool->GetTotalJobCount() * multiplier;
        PendingJobCount_ += pool->GetPendingJobCount() * multiplier;
        ActivePoolCount_ += static_cast<int>(!pool->IsCompleted()) * multiplier;

        if (!pool->IsCompleted() && pool->GetPendingJobCount() > 0) {
            if (multiplier == 1) {
                auto& poolIterator = PendingPoolIterators_[poolIndex];
                YT_VERIFY(poolIterator == PendingPools_.end());
                poolIterator = PendingPools_.insert(PendingPools_.begin(), poolIndex);
            } else {
                YT_VERIFY(multiplier == -1);
                auto& poolIterator = PendingPoolIterators_[poolIndex];
                YT_VERIFY(poolIterator != PendingPools_.end());
                PendingPools_.erase(poolIterator);
                poolIterator = PendingPools_.end();
            }
        }
    }

    class TPoolModificationGuard
    {
    public:
        TPoolModificationGuard(
            TMultiChunkPoolOutput* owner,
            int poolIndex)
            : Owner_(owner)
            , PoolIndex_(poolIndex)
        {
            Owner_->AddPoolStatistics(PoolIndex_, -1);
        }

        ~TPoolModificationGuard()
        {
            Owner_->AddPoolStatistics(PoolIndex_, +1);
        }

    private:
        TMultiChunkPoolOutput* Owner_;
        const int PoolIndex_;
    };

    //! It's required to create such a guard before any mutating actions with underlying pools
    //! to have cumulative statistics consistent with underlying pools' statistics.
    std::any MakeGuard(int poolIndex)
    {
        return std::make_any<TPoolModificationGuard>(this, poolIndex);
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TMultiChunkPoolOutput);

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkPool
    : public IChunkPool
    , public TMultiChunkPoolInput
    , public TMultiChunkPoolOutput
{
public:
    //! Used only for persistence.
    TMultiChunkPool() = default;

    TMultiChunkPool(
        std::vector<IChunkPoolInput*> underlyingPoolsInput,
        std::vector<IChunkPoolOutput*> underlyingPoolsOutput)
        : TMultiChunkPoolInput(std::move(underlyingPoolsInput))
        , TMultiChunkPoolOutput(std::move(underlyingPoolsOutput))
    {
        YT_VERIFY(underlyingPoolsInput.size() == underlyingPoolsOutput.size());
    }

    virtual void Persist(const TPersistenceContext& context)
    {
        TMultiChunkPoolInput::Persist(context);
        TMultiChunkPoolOutput::Persist(context);
    }

protected:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMultiChunkPool, 0xf7e412a9);

    virtual std::any MakeGuard(int poolIndex) override
    {
        return TMultiChunkPoolOutput::MakeGuard(poolIndex);
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TMultiChunkPool);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IChunkPoolInput> CreateMultiChunkPoolInput(
    std::vector<IChunkPoolInput*> underlyingPools)
{
    return std::make_unique<TMultiChunkPoolInput>(std::move(underlyingPools));
}

std::unique_ptr<IChunkPoolOutput> CreateMultiChunkPoolOutput(
    std::vector<IChunkPoolOutput*> underlyingPools)
{
    return std::make_unique<TMultiChunkPoolOutput>(std::move(underlyingPools));
}

std::unique_ptr<IChunkPool> CreateMultiChunkPool(
    std::vector<IChunkPool*> underlyingPools)
{
    std::vector<IChunkPoolInput*> inputPools(underlyingPools.begin(), underlyingPools.end());
    std::vector<IChunkPoolOutput*> outputPools(underlyingPools.begin(), underlyingPools.end());
    return std::make_unique<TMultiChunkPool>(std::move(inputPools), std::move(outputPools));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
