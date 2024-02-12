#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TCompactionHintFetchStatus
{
    bool IsFetchNeeded = false;
    bool InQueue = false;
    int RequestStep = 0;

    bool ShouldAddToQueue();
    bool ShouldMakeFirstRequest();
    bool ShouldConsumeRequestResult(int requestStep);
};

template <class TResult>
struct TCompactionHint
{
    TCompactionHintFetchStatus FetchStatus;
    std::optional<TResult> CompactionHint;
};

void Serialize(
    const TCompactionHintFetchStatus& compactionHintFetchStatus,
    NYson::IYsonConsumer* consumer);

template <class TResult>
void Serialize(
    const TCompactionHint<TResult>& compactionHint,
    NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TFetchCandidateQueue
{
public:
    explicit TFetchCandidateQueue(
        TCompactionHintFetcher* fetcher,
        NProfiling::TGauge sizeGauge);

    void Push(const IStorePtr& store);
    IStorePtr PopAndGet();
    void Clear();

    bool Empty() const;
    i64 Size() const;

private:
    void ChangeInQueueStatus(const IStorePtr& store, bool status);
    void UpdateSize();

    TCompactionHintFetcher* Fetcher_;
    NProfiling::TGauge SizeGauge_;
    std::queue<TWeakPtr<IStore>> Queue_;
};

////////////////////////////////////////////////////////////////////////////////

class TCompactionHintFetcher
    : public TRefCounted
{
public:
    TCompactionHintFetcher(
        IInvokerPtr invoker,
        NProfiling::TProfiler profiler);

    void FetchStoreInfos(
        TTablet* tablet,
        const std::optional<TRange<IStorePtr>>& stores = std::nullopt);

    void ResetCompactionHints(TTablet* tablet) const;

    virtual void Reconfigure(const NClusterNode::TClusterNodeDynamicConfigPtr& config) = 0;

protected:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    const IInvokerPtr Invoker_;
    const NConcurrency::TPeriodicExecutorPtr FetchingExecutor_;
    const NConcurrency::IReconfigurableThroughputThrottlerPtr RequestThrottler_;

    NProfiling::TProfiler Profiler_;
    const NProfiling::TCounter FinishedRequestCount_;

    void AddToQueue(const IStorePtr& store);

    void ClearQueue();

    void OnRequestFailed(
        const IStorePtr& store,
        int requestStep);

    bool IsValidStoreState(const IStorePtr& store);

private:
    const NProfiling::TCounter RequestCount_;
    const NProfiling::TCounter FailedRequestCount_;
    const NProfiling::TCounter ThrottledRequestCount_;

    TFetchCandidateQueue StoresQueue_;

    friend TFetchCandidateQueue;

    void GetStoresFromQueueAndMakeRequest();

    virtual void ResetResult(const IStorePtr& store) const = 0;

    virtual bool IsFetchableStore(const IStorePtr& store) const = 0;
    virtual bool IsFetchableTablet(const TTablet& tablet) const = 0;

    virtual TCompactionHintFetchStatus& GetFetchStatus(const IStorePtr& store) const = 0;

    virtual void MakeRequest(std::vector<IStorePtr>&& stores) = 0;
};

DEFINE_REFCOUNTED_TYPE(TCompactionHintFetcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

#define COMPACTION_HINT_FETCHER_INL_H_
#include "compaction_hint_fetcher-inl.h"
#undef COMPACTION_HINT_FETCHER_INL_H_
