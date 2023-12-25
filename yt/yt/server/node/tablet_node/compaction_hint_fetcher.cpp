#include "compaction_hint_fetcher.h"

#include "tablet.h"

namespace NYT::NTabletNode {

using namespace NProfiling;
using namespace NConcurrency;
using namespace NClusterNode;

////////////////////////////////////////////////////////////////////////////////

TCompactionHintFetcher::TCompactionHintFetcher(
    IInvokerPtr invoker,
    TProfiler profiler)
    : Invoker_(std::move(invoker))
    , RequestThrottler_(CreateReconfigurableThroughputThrottler(
        New<NConcurrency::TThroughputThrottlerConfig>()))
    , Profiler_(std::move(profiler))
    , RequestCount_(Profiler_.Counter("/request_count"))
    , FailedRequestCount_(Profiler_.Counter("/failed_request_count"))
    , ThrottledRequestCount_(Profiler_.Counter("/throttled_request_count"))
    , StoresQueue_(Profiler_.Gauge("/queue_size"))
{
    VERIFY_INVOKER_THREAD_AFFINITY(Invoker_, AutomatonThread);
}

void TCompactionHintFetcher::FetchStoreInfos(
    TTablet* tablet,
    const std::optional<TRange<IStorePtr>>& stores)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (!tablet || !IsFetchingRequiredForTablet(*tablet)) {
        return;
    }

    if (stores) {
        for (const auto& store : stores.value()) {
            AddToQueue(store);
        }
    } else {
        for (const auto& [storeId, store] : tablet->StoreIdMap()) {
            AddToQueue(store);
        }
    }

    TryMakeRequest();
}

void TCompactionHintFetcher::AddToQueue(const IStorePtr& store)
{
    if (IsFetchingRequiredForStore(store) &&
        (!HasRequestStatus(store) || GetRequestStatus(store) == ECompactionHintRequestStatus::None))
    {
        SetRequestStatus(store, ECompactionHintRequestStatus::InQueue);
        StoresQueue_.Push(MakeWeak(store));
    }
}

void TCompactionHintFetcher::ClearQueue()
{
    while (!StoresQueue_.Empty()) {
        if (auto store = StoresQueue_.PopAndGet().Lock()) {
            SetRequestStatus(store, ECompactionHintRequestStatus::None);
        }
    }
}

void TCompactionHintFetcher::OnRequestFailed(const IStorePtr& store)
{
    YT_VERIFY(HasRequestStatus(store));

    SetRequestStatus(store, ECompactionHintRequestStatus::None);
    AddToQueue(store);
    FailedRequestCount_.Increment();
}

std::vector<IStorePtr> TCompactionHintFetcher::GetStoresForRequest()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (StoresQueue_.Empty()) {
        return {};
    }

    i64 requestSize;
    while ((requestSize = RequestThrottler_->TryAcquireAvailable(StoresQueue_.Size())) == 0) {
        ThrottledRequestCount_.Increment();
        TDelayedExecutor::WaitForDuration(ThrottlerTryAcquireBackoff_);
        if (StoresQueue_.Empty()) {
            return {};
        }
    }

    std::vector<IStorePtr> stores;
    while (ssize(stores) < requestSize && !StoresQueue_.Empty()) {
        if (auto store = StoresQueue_.PopAndGet().Lock()) {
            stores.push_back(std::move(store));
        }
    }

    RequestCount_.Increment();

    return stores;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
