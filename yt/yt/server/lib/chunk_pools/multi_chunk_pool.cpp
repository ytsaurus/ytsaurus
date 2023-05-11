#include "multi_chunk_pool.h"
#include "chunk_pool.h"

#include <yt/yt/server/lib/controller_agent/progress_counter.h>

#include <yt/yt/ytlib/chunk_pools/chunk_stripe.h>

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

    explicit TMultiChunkPoolInput(std::vector<IPersistentChunkPoolInputPtr> underlyingPools)
        : UnderlyingPools_(std::move(underlyingPools))
    { }

    TExternalCookie Add(TChunkStripePtr stripe) override
    {
        YT_VERIFY(stripe->PartitionTag);
        auto poolIndex = *stripe->PartitionTag;
        auto* pool = Pool(poolIndex);
        auto cookie = pool->Add(std::move(stripe));

        return AddCookie(poolIndex, cookie);
    }

    TExternalCookie AddWithKey(TChunkStripePtr stripe, TChunkStripeKey key) override
    {
        YT_VERIFY(stripe->PartitionTag);
        auto poolIndex = *stripe->PartitionTag;
        auto* pool = Pool(poolIndex);
        auto cookie = pool->AddWithKey(std::move(stripe), key);

        return AddCookie(poolIndex, cookie);
    }

    void Suspend(TExternalCookie externalCookie) override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        auto* pool = Pool(poolIndex);
        pool->Suspend(cookie);
    }

    void Resume(TExternalCookie externalCookie) override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        auto* pool = Pool(poolIndex);
        pool->Resume(cookie);
    }

    void Reset(
        TExternalCookie externalCookie,
        TChunkStripePtr stripe,
        TInputChunkMappingPtr mapping) override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        stripe->PartitionTag = poolIndex;
        auto* cookiePool = Pool(poolIndex);

        cookiePool->Reset(cookie, std::move(stripe), std::move(mapping));
    }

    void FinishPool(int poolIndex) override
    {
        auto* pool = Pool(poolIndex);
        if (pool) {
            pool->Finish();
        }
    }

    void Finish() override
    {
        for (int poolIndex = 0; poolIndex < std::ssize(UnderlyingPools_); ++poolIndex) {
            FinishPool(poolIndex);
        }
        IsFinished_ = true;
    }

    bool IsFinished() const override
    {
        return IsFinished_;
    }

    void Persist(const TPersistenceContext& context) override
    {
        using ::NYT::Persist;

        Persist(context, UnderlyingPools_);
        Persist<TVectorSerializer<TTupleSerializer<std::pair<int, TCookie>, 2>>>(context, Cookies_);
        Persist(context, IsFinished_);
    }

protected:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMultiChunkPoolInput, 0xe712ad5b);

    std::vector<IPersistentChunkPoolInputPtr> UnderlyingPools_;

    //! Mapping external_cookie -> cookie_descriptor.
    std::vector<TCookieDescriptor> Cookies_;

    bool IsFinished_ = false;

    TCookieDescriptor Cookie(TExternalCookie externalCookie) const
    {
        YT_VERIFY(externalCookie >= 0);
        YT_VERIFY(externalCookie < std::ssize(Cookies_));

        return Cookies_[externalCookie];
    }

    TExternalCookie AddCookie(int poolIndex, TCookie cookie)
    {
        auto externalCookie = Cookies_.size();
        Cookies_.emplace_back(poolIndex, cookie);

        return externalCookie;
    }

    IPersistentChunkPoolInput* Pool(int poolIndex)
    {
        YT_VERIFY(poolIndex >= 0);
        YT_VERIFY(poolIndex < std::ssize(UnderlyingPools_));

        return UnderlyingPools_[poolIndex].Get();
    }

    void AddPoolInput(IPersistentChunkPoolInputPtr pool, int poolIndex)
    {
        if (poolIndex >= std::ssize(UnderlyingPools_)) {
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
    , public TJobSplittingBase
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(NChunkClient::TInputChunkPtr, std::any tag), ChunkTeleported);
    DEFINE_SIGNAL_OVERRIDE(void(), Completed);
    DEFINE_SIGNAL_OVERRIDE(void(), Uncompleted);

public:
    using TExternalCookie = TCookie;

    //! Pair of underlying pool and its cookie uniquely representing cookie in multi chunk pool.
    using TCookieDescriptor = std::pair<int, TCookie>;

    //! Used only for persistence.
    TMultiChunkPoolOutput() = default;

    explicit TMultiChunkPoolOutput(std::vector<IPersistentChunkPoolOutputPtr> underlyingPools)
    {
        UnderlyingPools_.reserve(underlyingPools.size());
        PendingPoolIterators_.reserve(underlyingPools.size());

        for (int poolIndex = 0; poolIndex < std::ssize(underlyingPools); ++poolIndex) {
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

    TOutputOrderPtr GetOutputOrder() const override
    {
        return nullptr;
    }

    i64 GetLocality(TNodeId /*nodeId*/) const override
    {
        return 0;
    }

    NTableClient::TChunkStripeStatisticsVector GetApproximateStripeStatistics() const override
    {
        if (PendingPools_.empty()) {
            return {};
        }

        return CurrentPool()->GetApproximateStripeStatistics();
    }

    TExternalCookie Extract(TNodeId nodeId) override
    {
        int poolIndex = -1;
        auto cookie = IChunkPoolOutput::NullCookie;

        if (PendingPools_.empty()) {
            if (BlockedPools_.empty()) {
                return IChunkPoolOutput::NullCookie;
            } else {
                poolIndex = *BlockedPools_.begin();
                auto* pool = Pool(poolIndex);
                cookie = pool->Extract(nodeId);
                YT_VERIFY(cookie != IChunkPoolOutput::NullCookie);
            }
        } else {
            poolIndex = CurrentPoolIndex();
            cookie = Pool(poolIndex)->Extract(nodeId);
        }

        YT_VERIFY(poolIndex != -1);
        YT_VERIFY(cookie != IChunkPoolOutput::NullCookie);

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

    TExternalCookie ExtractFromPool(int underlyingPoolIndexHint, TNodeId nodeId) override
    {
        // NB(gritukan): SortedMerge task can try to extract cookie from partition
        // that was not registered in its pool yet. Do not crash in this case.
        if (underlyingPoolIndexHint >= std::ssize(UnderlyingPools_)) {
            return Extract(nodeId);
        }

        auto* pool = Pool(underlyingPoolIndexHint);
        if (!pool) {
            return Extract(nodeId);
        }

        auto cookie = pool->Extract(nodeId);
        if (cookie == IChunkPoolOutput::NullCookie) {
            return Extract(nodeId);
        } else {
            auto externalCookie = Cookies_.size();
            Cookies_.emplace_back(underlyingPoolIndexHint, cookie);

            return externalCookie;
        }
    }

    TChunkStripeListPtr GetStripeList(TExternalCookie externalCookie) override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        auto stripeList = Pool(poolIndex)->GetStripeList(cookie);
        stripeList->PartitionTag = poolIndex;

        return stripeList;
    }

    bool IsCompleted() const override
    {
        return IsCompleted_;
    }

    int GetStripeListSliceCount(TExternalCookie externalCookie) const override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        return Pool(poolIndex)->GetStripeListSliceCount(cookie);
    }

    void Completed(TExternalCookie externalCookie, const TCompletedJobSummary& jobSummary) override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        Pool(poolIndex)->Completed(cookie, jobSummary);
    }

    void Failed(TExternalCookie externalCookie) override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        Pool(poolIndex)->Failed(cookie);
    }

    void Aborted(TExternalCookie externalCookie, EAbortReason reason) override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        Pool(poolIndex)->Aborted(cookie, reason);
    }

    void Lost(TExternalCookie externalCookie) override
    {
        auto [poolIndex, cookie] = Cookie(externalCookie);
        Pool(poolIndex)->Lost(cookie);
    }

    void Finalize() override
    {
        Finalized_ = true;
        CheckCompleted();
    }

    void AddPoolOutput(IPersistentChunkPoolOutputPtr pool, int poolIndex) override
    {
        YT_VERIFY(pool);
        YT_VERIFY(!pool->GetOutputOrder());

        if (poolIndex >= std::ssize(UnderlyingPools_)) {
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

    void Persist(const TPersistenceContext& context) override
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

    std::vector<IPersistentChunkPoolOutputPtr> UnderlyingPools_;

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

    const IPersistentChunkPoolOutput* Pool(int poolIndex) const
    {
        YT_VERIFY(poolIndex >= 0);
        YT_VERIFY(poolIndex < std::ssize(UnderlyingPools_));

        return UnderlyingPools_[poolIndex].Get();
    }

    IPersistentChunkPoolOutput* Pool(int poolIndex)
    {
        YT_VERIFY(poolIndex >= 0);
        YT_VERIFY(poolIndex < std::ssize(UnderlyingPools_));

        return UnderlyingPools_[poolIndex].Get();
    }

    int CurrentPoolIndex() const
    {
        YT_VERIFY(!PendingPools_.empty());

        return *PendingPools_.begin();
    }

    const IPersistentChunkPoolOutput* CurrentPool() const
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
            BIND_NO_PROPAGATE(&TMultiChunkPoolOutput::OnUnderlyingPoolPendingJobCountChanged, Unretained(this), poolIndex));
        pool->GetJobCounter()->SubscribeBlockedUpdated(
            BIND_NO_PROPAGATE(&TMultiChunkPoolOutput::OnUnderlyingPoolBlockedJobCountChanged, Unretained(this), poolIndex));
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
        std::vector<IPersistentChunkPoolInputPtr> underlyingPoolsInput,
        std::vector<IPersistentChunkPoolOutputPtr> underlyingPoolsOutput)
        : TMultiChunkPoolInput(std::move(underlyingPoolsInput))
        , TMultiChunkPoolOutput(std::move(underlyingPoolsOutput))
    {
        YT_VERIFY(underlyingPoolsInput.size() == underlyingPoolsOutput.size());
    }

    void AddPool(IPersistentChunkPoolPtr pool, int poolIndex) override
    {
        AddPoolInput(pool, poolIndex);
        AddPoolOutput(std::move(pool), poolIndex);
    }

    void Persist(const TPersistenceContext& context) override
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
    std::vector<IPersistentChunkPoolInputPtr> underlyingPools)
{
    return New<TMultiChunkPoolInput>(std::move(underlyingPools));
}

IMultiChunkPoolOutputPtr CreateMultiChunkPoolOutput(
    std::vector<IPersistentChunkPoolOutputPtr> underlyingPools)
{
    return New<TMultiChunkPoolOutput>(std::move(underlyingPools));
}

IMultiChunkPoolPtr CreateMultiChunkPool(
    std::vector<IPersistentChunkPoolPtr> underlyingPools)
{
    std::vector<IPersistentChunkPoolInputPtr> inputPools(underlyingPools.begin(), underlyingPools.end());
    std::vector<IPersistentChunkPoolOutputPtr> outputPools(underlyingPools.begin(), underlyingPools.end());
    return New<TMultiChunkPool>(std::move(inputPools), std::move(outputPools));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
