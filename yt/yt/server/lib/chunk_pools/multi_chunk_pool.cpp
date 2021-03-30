#include "multi_chunk_pool.h"
#include "chunk_pool.h"
#include "chunk_stripe.h"

#include <yt/yt/server/lib/controller_agent/progress_counter.h>

#include <util/generic/noncopyable.h>

namespace NYT::NChunkPools {

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
        auto cookie = pool->Add(std::move(stripe));

        return AddCookie(poolIndex, cookie);
    }

    virtual TExternalCookie AddWithKey(TChunkStripePtr stripe, TChunkStripeKey key)
    {
        YT_VERIFY(stripe->PartitionTag);
        auto poolIndex = *stripe->PartitionTag;
        auto* pool = Pool(poolIndex);
        auto cookie = pool->AddWithKey(std::move(stripe), key);

        return AddCookie(poolIndex, cookie);
    }

    virtual void Suspend(TExternalCookie externalCookie) override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        auto* pool = Pool(poolIndex);
        pool->Suspend(cookie);
    }

    virtual void Resume(TExternalCookie externalCookie) override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        auto* pool = Pool(poolIndex);
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

        cookiePool->Reset(cookie, std::move(stripe), std::move(mapping));
    }

    virtual void FinishPool(int poolIndex) override
    {
        auto* pool = Pool(poolIndex);
        if (pool) {
            pool->Finish();
        }
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
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMultiChunkPoolInput, 0xe712ad5b);

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

    void AddPoolInput(IChunkPoolInputPtr pool, int poolIndex)
    {
        if (poolIndex >= UnderlyingPools_.size()) {
            UnderlyingPools_.resize(poolIndex + 1);
        }

        YT_VERIFY(!UnderlyingPools_[poolIndex]);
        UnderlyingPools_[poolIndex] = std::move(pool);
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TMultiChunkPoolInput);

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkPoolOutput
    : public virtual IMultiChunkPoolOutput
{
public:
    DEFINE_SIGNAL(void(NChunkClient::TInputChunkPtr, std::any tag), ChunkTeleported);
    DEFINE_SIGNAL(void(), Completed);
    DEFINE_SIGNAL(void(), Uncompleted);

public:
    using TExternalCookie = TCookie;

    //! Pair of underlying pool and its cookie uniquely representing cookie in multi chunk pool.
    using TCookieDescriptor = std::pair<int, TCookie>;

    //! Used only for persistence.
    TMultiChunkPoolOutput() = default;

    explicit TMultiChunkPoolOutput(std::vector<IChunkPoolOutputPtr> underlyingPools)
    {
        UnderlyingPools_.reserve(underlyingPools.size());
        PendingPoolIterators_.reserve(underlyingPools.size());

        for (int poolIndex = 0; poolIndex < underlyingPools.size(); ++poolIndex) {
            const auto& pool = underlyingPools[poolIndex];
            AddPoolOutput(std::move(pool), poolIndex);
        }

        CheckCompleted();
    }

    const TProgressCounterPtr& GetJobCounter() const override
    {
        return JobCounter_;
    }

    const TProgressCounterPtr& GetDataWeightCounter() const override
    {
        return DataWeightCounter_;
    }

    const TProgressCounterPtr& GetRowCounter() const override
    {
        return RowCounter_;
    }

    const TProgressCounterPtr& GetDataSliceCounter() const override
    {
        return DataSliceCounter_;
    }

    virtual TOutputOrderPtr GetOutputOrder() const override
    {
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
        int poolIndex = -1;
        auto cookie = NullCookie;

        if (PendingPools_.empty()) {
            if (BlockedPools_.empty()) {
                return NullCookie;
            } else {
                poolIndex = *BlockedPools_.begin();
                auto* pool = Pool(poolIndex);
                cookie = pool->Extract(nodeId);
                YT_VERIFY(cookie != NullCookie);
            }
        } else {
            poolIndex = CurrentPoolIndex();
            cookie = Pool(poolIndex)->Extract(nodeId);
        }

        YT_VERIFY(poolIndex != -1);
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

    virtual TExternalCookie ExtractFromPool(int underlyingPoolIndexHint, TNodeId nodeId) override
    {
        // NB(gritukan): SortedMerge task can try to extract cookie from partition
        // that was not registered in its pool yet. Do not crash in this case.
        if (underlyingPoolIndexHint >= UnderlyingPools_.size()) {
            return Extract(nodeId);
        }

        auto* pool = Pool(underlyingPoolIndexHint);
        if (!pool) {
            return Extract(nodeId);
        }

        auto cookie = pool->Extract(nodeId);
        if (cookie == NullCookie) {
            return Extract(nodeId);
        } else {
            auto externalCookie = Cookies_.size();
            Cookies_.emplace_back(underlyingPoolIndexHint, cookie);

            return externalCookie;
        }
    }

    virtual TChunkStripeListPtr GetStripeList(TExternalCookie externalCookie) override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        auto stripeList = Pool(poolIndex)->GetStripeList(cookie);
        stripeList->PartitionTag = poolIndex;

        return stripeList;
    }

    virtual bool IsCompleted() const override
    {
        return IsCompleted_;
    }

    virtual int GetStripeListSliceCount(TExternalCookie externalCookie) const override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        return Pool(poolIndex)->GetStripeListSliceCount(cookie);
    }

    virtual void Completed(TExternalCookie externalCookie, const TCompletedJobSummary& jobSummary) override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        Pool(poolIndex)->Completed(cookie, jobSummary);
    }

    virtual void Failed(TExternalCookie externalCookie) override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        Pool(poolIndex)->Failed(cookie);
    }

    virtual void Aborted(TExternalCookie externalCookie, EAbortReason reason) override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        Pool(poolIndex)->Aborted(cookie, reason);
    }

    virtual void Lost(TExternalCookie externalCookie) override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        Pool(poolIndex)->Lost(cookie);
    }

    virtual void Finalize() override
    {
        Finalized_ = true;
        CheckCompleted();
    }

    virtual void AddPoolOutput(IChunkPoolOutputPtr pool, int poolIndex) override
    {
        YT_VERIFY(pool);
        YT_VERIFY(!pool->GetOutputOrder());

        if (poolIndex >= UnderlyingPools_.size()) {
            UnderlyingPools_.resize(poolIndex + 1);
            PendingPoolIterators_.resize(poolIndex + 1, PendingPools_.end());
        }

        YT_VERIFY(!UnderlyingPools_[poolIndex]);
        YT_VERIFY(PendingPoolIterators_[poolIndex] == PendingPools_.end());

        pool->GetJobCounter()->AddParent(JobCounter_);
        pool->GetDataWeightCounter()->AddParent(DataWeightCounter_);
        pool->GetRowCounter()->AddParent(RowCounter_);
        pool->GetDataSliceCounter()->AddParent(DataSliceCounter_);

        if (!pool->IsCompleted()) {
            ++ActivePoolCount_;
        }

        UnderlyingPools_[poolIndex] = std::move(pool);

        SetupCallbacks(poolIndex);
        OnUnderlyingPoolPendingJobCountChanged(poolIndex);
        OnUnderlyingPoolBlockedJobCountChanged(poolIndex);
    }

    virtual void Persist(const TPersistenceContext& context) override
    {
        using ::NYT::Persist;

        Persist(context, UnderlyingPools_);
        Persist(context, ActivePoolCount_);
        Persist(context, JobCounter_);
        Persist(context, DataWeightCounter_);
        Persist(context, RowCounter_);
        Persist(context, DataSliceCounter_);
        Persist<TVectorSerializer<TTupleSerializer<std::pair<int, TCookie>, 2>>>(context, Cookies_);
        Persist<TMapSerializer<TTupleSerializer<std::pair<int, TCookie>, 2>, TDefaultSerializer, TUnsortedTag>>(context, CookieDescriptorToExternalCookie_);
        Persist(context, BlockedPools_);
        Persist(context, Finalized_);
        Persist(context, IsCompleted_);

        if (context.IsLoad()) {
            // NB(gritukan): It seems hard to persist list iterators, so we do not persist statistics
            // and restore them from scratch here using underlying pools statistics.
            PendingPoolIterators_ = std::vector<std::list<int>::iterator>(UnderlyingPools_.size(), PendingPools_.end());
            for (int poolIndex = static_cast<int>(UnderlyingPools_.size()) - 1; poolIndex >= 0; --poolIndex) {
                if (Pool(poolIndex)) {
                    SetupCallbacks(poolIndex);
                    OnUnderlyingPoolPendingJobCountChanged(poolIndex);
                    OnUnderlyingPoolBlockedJobCountChanged(poolIndex);
                }
            }
        }
    }

protected:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMultiChunkPoolOutput, 0x135ffa20);

    std::vector<IChunkPoolOutputPtr> UnderlyingPools_;

    //! Number of uncompleted pools.
    int ActivePoolCount_ = 0;

    //! Parent of all underlying pool job counters.
    TProgressCounterPtr JobCounter_ = New<TProgressCounter>();

    //! Parent of all underlying pool data weight counters.
    TProgressCounterPtr DataWeightCounter_ = New<TProgressCounter>();

    //! Parent of all underlying pool row counters.
    TProgressCounterPtr RowCounter_ = New<TProgressCounter>();

    //! Parent of all underlying pool data slice counters.
    TProgressCounterPtr DataSliceCounter_ = New<TProgressCounter>();

    //! External cookie -> internal cookie.
    std::vector<TCookieDescriptor> Cookies_;

    //! Internal cookie -> external cookie.
    THashMap<TCookieDescriptor, TExternalCookie> CookieDescriptorToExternalCookie_;

    //! Queue of pools with pending jobs.
    std::list<int> PendingPools_;

    //! Mapping pool_index -> iterator in pending_pools for pools with pending jobs.
    std::vector<std::list<int>::iterator> PendingPoolIterators_;

    //! Pools with blocked jobs.
    // NB: Unlike pending pool set it is not required to be a queue
    // so we use THashSet for the sake of simplicity.
    THashSet<int> BlockedPools_;

    //! If true, no new underlying pool will be added.
    bool Finalized_ = false;

    //! Whether pool is completed.
    bool IsCompleted_ = false;

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

    void CheckCompleted()
    {
        bool completed = Finalized_ && (ActivePoolCount_ == 0);

        if (!IsCompleted_ && completed) {
            Completed_.Fire();
        } else if (IsCompleted_ && !completed) {
            Uncompleted_.Fire();
        }

        IsCompleted_ = completed;
    }

    void SetupCallbacks(int poolIndex)
    {
        auto* pool = Pool(poolIndex);
        pool->SubscribeChunkTeleported(
            BIND([this, poolIndex] (TInputChunkPtr teleportChunk, std::any /*tag*/) {
                ChunkTeleported_.Fire(std::move(teleportChunk), poolIndex);
            }));
        pool->SubscribeCompleted(BIND([this] {
            --ActivePoolCount_;
            YT_VERIFY(ActivePoolCount_ >= 0);
            CheckCompleted();
        }));
        pool->SubscribeUncompleted(BIND([this] {
            ++ActivePoolCount_;
            CheckCompleted();
        }));
        pool->GetJobCounter()->SubscribePendingUpdated(
            BIND(&TMultiChunkPoolOutput::OnUnderlyingPoolPendingJobCountChanged, Unretained(this), poolIndex));
        pool->GetJobCounter()->SubscribeBlockedUpdated(
            BIND(&TMultiChunkPoolOutput::OnUnderlyingPoolBlockedJobCountChanged, Unretained(this), poolIndex));
    }

    void OnUnderlyingPoolPendingJobCountChanged(int poolIndex)
    {
        auto* pool = Pool(poolIndex);
        auto& poolIterator = PendingPoolIterators_[poolIndex];
        bool hasPendingJobs = pool->GetJobCounter()->GetPending() > 0;
        if (poolIterator == PendingPools_.end() && hasPendingJobs) {
            poolIterator = PendingPools_.insert(PendingPools_.begin(), poolIndex);
        } else if (poolIterator != PendingPools_.end() && !hasPendingJobs) {
            PendingPools_.erase(poolIterator);
            poolIterator = PendingPools_.end();
        }
    }

    void OnUnderlyingPoolBlockedJobCountChanged(int poolIndex)
    {
        auto* pool = Pool(poolIndex);
        bool hasBlockedJobs = pool->GetJobCounter()->GetBlocked() > 0;
        if (hasBlockedJobs) {
            BlockedPools_.insert(poolIndex);
        } else {
            BlockedPools_.erase(poolIndex);
        }
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

    virtual void AddPool(IChunkPoolPtr pool, int poolIndex) override
    {
        AddPoolInput(pool, poolIndex);
        AddPoolOutput(std::move(pool), poolIndex);
    }

    virtual void Persist(const TPersistenceContext& context)
    {
        TMultiChunkPoolInput::Persist(context);
        TMultiChunkPoolOutput::Persist(context);
    }

protected:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMultiChunkPool, 0xf7e412a9);
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

} // namespace NYT::NChunkPools
