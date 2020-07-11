#include "multi_chunk_pool.h"
#include "chunk_pool.h"
#include "chunk_stripe.h"

#include <util/generic/noncopyable.h>

namespace NYT::NLegacyChunkPools {

using namespace NChunkClient;
using namespace NControllerAgent;
using namespace NNodeTrackerClient;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkPoolInput
    : public virtual IMultiChunkPoolInput
{
public:
    using TExternalCookie = TCookie;

    //! Pair of underlying pool index and its cookie uniquely representing cookie in multi chunk pool.
    using TCookieDescriptor = std::pair<int, TCookie>;

    //! Used only for persistence.
    TMultiChunkPoolInput() = default;

    explicit TMultiChunkPoolInput(std::vector<IChunkPoolInputPtr> underlyingPools)
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

    virtual void FinishPool(int poolIndex) override
    {
        auto* pool = Pool(poolIndex);
        auto guard = MakeGuard(poolIndex);
        pool->Finish();
    }

    virtual void Finish() override
    {
        for (int poolIndex = 0; poolIndex < UnderlyingPools_.size(); ++poolIndex) {
            FinishPool(poolIndex);
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
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMultiChunkPoolInput, 0xe712ad5c);

    std::vector<IChunkPoolInputPtr> UnderlyingPools_;

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

        return UnderlyingPools_[poolIndex].Get();
    }

    void AddPoolInput(IChunkPoolInputPtr pool)
    {
        UnderlyingPools_.emplace_back(std::move(pool));
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
    : public virtual IMultiChunkPoolOutput
{
public:
    using TExternalCookie = TCookie;

    //! Pair of underlying pool and its cookie uniquely representing cookie in multi chunk pool.
    using TCookieDescriptor = std::pair<int, TCookie>;

    //! Used only for persistence.
    TMultiChunkPoolOutput() = default;

    explicit TMultiChunkPoolOutput(std::vector<IChunkPoolOutputPtr> underlyingPools)
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

    const TLegacyProgressCounterPtr& GetJobCounter() const override
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

        TCookieDescriptor cookieDescriptor(poolIndex, cookie);
        auto cookieIt = CookieDescriptorToExternalCookie_.find(cookieDescriptor);
        if (cookieIt == CookieDescriptorToExternalCookie_.end()) {
            auto externalCookie = Cookies_.size();
            Cookies_.emplace_back(poolIndex, cookie);
            YT_VERIFY(CookieDescriptorToExternalCookie_.emplace(cookieDescriptor, externalCookie).second);
            return externalCookie;
        } else {
            return cookieIt->second;
        }
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
        return Finalized_ && ActivePoolCount_ == 0;
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

    virtual void Finalize() override
    {
        Finalized_ = true;
    }

    virtual void AddPoolOutput(IChunkPoolOutputPtr pool) override
    {
        auto poolIndex = UnderlyingPools_.size();
        pool->SubscribeChunkTeleported(
            BIND([this, poolIndex] (TInputChunkPtr teleportChunk, std::any /*tag*/) {
                ChunkTeleported_.Fire(std::move(teleportChunk), poolIndex);
            }));
        UnderlyingPools_.emplace_back(std::move(pool));
        PendingPoolIterators_.push_back(PendingPools_.end());
        YT_VERIFY(UnderlyingPools_.size() == PendingPoolIterators_.size());
        AddPoolStatistics(poolIndex, +1);
    }

    virtual void Persist(const TPersistenceContext& context) override
    {
        using ::NYT::Persist;

        Persist(context, UnderlyingPools_);
        Persist(context, JobCounter_);
        Persist<TVectorSerializer<TTupleSerializer<std::pair<int, TCookie>, 2>>>(context, Cookies_);
        Persist<TMapSerializer<TTupleSerializer<std::pair<int, TCookie>, 2>, TDefaultSerializer, TUnsortedTag>>(context, CookieDescriptorToExternalCookie_);
        Persist(context, Finalized_);

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
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMultiChunkPoolOutput, 0x135ffa21);

    std::vector<IChunkPoolOutputPtr> UnderlyingPools_;

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
    TLegacyProgressCounterPtr JobCounter_ = New<TLegacyProgressCounter>(0);

    //! Sum of data slice count over all underlying pools.
    i64 DataSliceCount_ = 0;

    //! Sum of total job count over all underlying pools.
    int TotalJobCount_ = 0;

    //! Sum of pending job count over all underlying pools.
    int PendingJobCount_ = 0;

    //! Number of active (incomplete) chunk pools.
    int ActivePoolCount_ = 0;

    //! External cookie -> internal cookie.
    std::vector<TCookieDescriptor> Cookies_;

    //! Internal cookie -> external cookie.
    THashMap<TCookieDescriptor, TExternalCookie> CookieDescriptorToExternalCookie_;

    //! Queue of pools with pending jobs.
    std::list<int> PendingPools_;

    //! Mapping pool_index -> iterator in pending_pools for pools with pending jobs.
    std::vector<std::list<int>::iterator> PendingPoolIterators_;

    //! If true, no new underlying pool will be added.
    bool Finalized_ = false;

    const IChunkPoolOutput* Pool(int poolIndex) const
    {
        YT_VERIFY(poolIndex >= 0);
        YT_VERIFY(poolIndex < UnderlyingPools_.size());

        return UnderlyingPools_[poolIndex].Get();
    }

    IChunkPoolOutput* Pool(int poolIndex)
    {
        YT_VERIFY(poolIndex >= 0);
        YT_VERIFY(poolIndex < UnderlyingPools_.size());

        return UnderlyingPools_[poolIndex].Get();
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
    : public IMultiChunkPool
    , public TMultiChunkPoolInput
    , public TMultiChunkPoolOutput
{
public:
    //! Used only for persistence.
    TMultiChunkPool() = default;

    TMultiChunkPool(
        std::vector<IChunkPoolInputPtr> underlyingPoolsInput,
        std::vector<IChunkPoolOutputPtr> underlyingPoolsOutput)
        : TMultiChunkPoolInput(std::move(underlyingPoolsInput))
        , TMultiChunkPoolOutput(std::move(underlyingPoolsOutput))
    {
        YT_VERIFY(underlyingPoolsInput.size() == underlyingPoolsOutput.size());
    }

    virtual void AddPool(IChunkPoolPtr pool) override
    {
        AddPoolInput(pool);
        AddPoolOutput(std::move(pool));
    }

    virtual void Persist(const TPersistenceContext& context)
    {
        TMultiChunkPoolInput::Persist(context);
        TMultiChunkPoolOutput::Persist(context);
    }

protected:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMultiChunkPool, 0xf7e412aa);

    virtual std::any MakeGuard(int poolIndex) override
    {
        return TMultiChunkPoolOutput::MakeGuard(poolIndex);
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TMultiChunkPool);

////////////////////////////////////////////////////////////////////////////////

IMultiChunkPoolInputPtr CreateMultiChunkPoolInput(
    std::vector<IChunkPoolInputPtr> underlyingPools)
{
    return New<TMultiChunkPoolInput>(std::move(underlyingPools));
}

IMultiChunkPoolOutputPtr CreateMultiChunkPoolOutput(
    std::vector<IChunkPoolOutputPtr> underlyingPools)
{
    return New<TMultiChunkPoolOutput>(std::move(underlyingPools));
}

IMultiChunkPoolPtr CreateMultiChunkPool(
    std::vector<IChunkPoolPtr> underlyingPools)
{
    std::vector<IChunkPoolInputPtr> inputPools(underlyingPools.begin(), underlyingPools.end());
    std::vector<IChunkPoolOutputPtr> outputPools(underlyingPools.begin(), underlyingPools.end());
    return New<TMultiChunkPool>(std::move(inputPools), std::move(outputPools));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLegacyChunkPools
