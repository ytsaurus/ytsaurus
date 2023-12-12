#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/lsm/store.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

template <class TResult>
class TCompactionHintRequestWithResult
{
public:
    void SetRequestStatus(ECompactionHintRequestStatus status);
    void SetResult(TResult result);

    bool IsRequestStatus() const;
    bool IsCertainRequestStatus(ECompactionHintRequestStatus status) const;

    ECompactionHintRequestStatus AsRequestStatus() const;
    const TResult& AsResult() const;

private:
    std::variant<ECompactionHintRequestStatus, TResult> Data_;
};

template <class TResult>
void Serialize(
    const TCompactionHintRequestWithResult<TResult>& hint,
    NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TSortedChunkStoreCompactionHints
{
    TCompactionHintRequestWithResult<NLsm::TRowDigestUpcomingCompactionInfo> RowDigest;
    TCompactionHintRequestWithResult<EChunkViewSizeStatus> ChunkViewSize;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TProfiledQueue
{
public:
    explicit TProfiledQueue(NProfiling::TGauge sizeCounter);

    void Push(T value);
    template <class ...Args>
    void Emplace(Args&& ...args);

    void Pop();
    T PopAndGet();

    i64 Size() const;
    bool Empty() const;

private:
    void UpdateSize();

    std::queue<T> Queue_;
    NProfiling::TGauge SizeCounter_;
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

    virtual void Reconfigure(
        const NClusterNode::TClusterNodeDynamicConfigPtr& oldConfig,
        const NClusterNode::TClusterNodeDynamicConfigPtr& newConfig) = 0;

protected:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    const IInvokerPtr Invoker_;

    const NConcurrency::IReconfigurableThroughputThrottlerPtr RequestThrottler_;

    TDuration ThrottlerTryAcquireBackoff_;

    NProfiling::TProfiler Profiler_;

    void AddToQueue(const IStorePtr& store);

    void ClearQueue();

    void OnRequestFailed(const IStorePtr& store);

    std::vector<IStorePtr> GetStoresForRequest();

private:
    const NProfiling::TCounter RequestCount_;
    const NProfiling::TCounter FailedRequestCount_;
    const NProfiling::TCounter ThrottledRequestCount_;

    TProfiledQueue<TWeakPtr<IStore>> StoresQueue_;

    virtual bool HasRequestStatus(const IStorePtr& store) const = 0;
    virtual ECompactionHintRequestStatus GetRequestStatus(const IStorePtr& store) const = 0;
    virtual void SetRequestStatus(const IStorePtr& store, ECompactionHintRequestStatus status) const = 0;

    virtual bool IsFetchingRequiredForStore(const IStorePtr& store) const = 0;
    virtual bool IsFetchingRequiredForTablet(const TTablet& tablet) const = 0;

    virtual void TryMakeRequest() = 0;
};

DEFINE_REFCOUNTED_TYPE(TCompactionHintFetcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

#define COMPACTION_HINT_FETCHER_INL_H_
#include "compaction_hint_fetcher-inl.h"
#undef COMPACTION_HINT_FETCHER_INL_H_
