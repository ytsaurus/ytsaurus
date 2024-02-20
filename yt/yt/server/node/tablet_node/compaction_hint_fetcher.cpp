#include "compaction_hint_fetcher.h"

#include "tablet.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NTabletNode {

using namespace NYTree;
using namespace NProfiling;
using namespace NConcurrency;
using namespace NClusterNode;

////////////////////////////////////////////////////////////////////////////////

bool TCompactionHintFetchStatus::ShouldAddToQueue()
{
    return IsFetchNeeded && !InQueue && RequestStep == 0;
}

bool TCompactionHintFetchStatus::ShouldMakeFirstRequest()
{
    YT_VERIFY(!InQueue);
    // We could consume request result from previos epoch.
    return IsFetchNeeded && RequestStep == 0;
}

bool TCompactionHintFetchStatus::ShouldConsumeRequestResult(int requestStep)
{
    return IsFetchNeeded && requestStep > RequestStep;
}

void Serialize(
    const TCompactionHintFetchStatus& compactionHintFetchStatus,
    NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("is_fetch_needed").Value(compactionHintFetchStatus.IsFetchNeeded)
            .Item("in_queue").Value(compactionHintFetchStatus.InQueue)
            .Item("request_step").Value(compactionHintFetchStatus.RequestStep)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

TFetchCandidateQueue::TFetchCandidateQueue(
    TCompactionHintFetcher* fetcher,
    NProfiling::TGauge sizeGauge)
    : Fetcher_(fetcher)
    , SizeGauge_(std::move(sizeGauge))
{ }

void TFetchCandidateQueue::Push(const IStorePtr& store)
{
    Queue_.push(store);
    ChangeInQueueStatus(store, true);
    UpdateSize();
}

IStorePtr TFetchCandidateQueue::PopAndGet()
{
    IStorePtr store = Queue_.front().Lock();

    ChangeInQueueStatus(store, false);
    Queue_.pop();
    UpdateSize();

    return store;
}

void TFetchCandidateQueue::Clear()
{
    while (!Queue_.empty()) {
        ChangeInQueueStatus(Queue_.back().Lock(), false);
        Queue_.pop();
    }
    UpdateSize();
}

bool TFetchCandidateQueue::Empty() const
{
    return Queue_.empty();
}

i64 TFetchCandidateQueue::Size() const
{
    return ssize(Queue_);
}

void TFetchCandidateQueue::ChangeInQueueStatus(const IStorePtr& store, bool status)
{
    if (store) {
        bool& inQueue = Fetcher_->GetFetchStatus(store).InQueue;
        YT_VERIFY(inQueue != status);
        inQueue = status;
    }
}

void TFetchCandidateQueue::UpdateSize()
{
    SizeGauge_.Update(Queue_.size());
}

////////////////////////////////////////////////////////////////////////////////

TCompactionHintFetcher::TCompactionHintFetcher(
    IInvokerPtr invoker,
    TProfiler profiler)
    : Invoker_(std::move(invoker))
    , FetchingExecutor_(New<TPeriodicExecutor>(
        Invoker_,
        BIND(&TCompactionHintFetcher::GetStoresFromQueueAndMakeRequest, MakeWeak(this))))
    , RequestThrottler_(CreateReconfigurableThroughputThrottler(
        New<NConcurrency::TThroughputThrottlerConfig>()))
    , Profiler_(std::move(profiler))
    , FinishedRequestCount_(Profiler_.Counter("/finished_request_count"))
    , RequestCount_(Profiler_.Counter("/request_count"))
    , FailedRequestCount_(Profiler_.Counter("/failed_request_count"))
    , ThrottledRequestCount_(Profiler_.Counter("/throttled_request_count"))
    , StoresQueue_(this, Profiler_.Gauge("/queue_size"))
{
    VERIFY_INVOKER_THREAD_AFFINITY(Invoker_, AutomatonThread);
    FetchingExecutor_->Start();
}

void TCompactionHintFetcher::FetchStoreInfos(
    TTablet* tablet,
    const std::optional<TRange<IStorePtr>>& stores)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (!tablet || !IsFetchableTablet(*tablet)) {
        return;
    }

    auto handleStore = [&] (const IStorePtr& store) {
        if (IsFetchableStore(store)) {
            GetFetchStatus(store).IsFetchNeeded = true;
            AddToQueue(store);
        }
    };

    if (stores) {
        for (const auto& store : stores.value()) {
            handleStore(store);
        }
    } else {
        for (const auto& [storeId, store] : tablet->StoreIdMap()) {
            handleStore(store);
        }
    }
}

void TCompactionHintFetcher::ResetCompactionHints(TTablet* tablet) const
{
    for (const auto& [storeId, store] : tablet->StoreIdMap()) {
        if (IsFetchableStore(store)) {
            auto& fetchStatus = GetFetchStatus(store);
            fetchStatus.IsFetchNeeded = false;
            fetchStatus.RequestStep = 0;

            ResetResult(store);
        }
    }
}

void TCompactionHintFetcher::AddToQueue(const IStorePtr& store)
{
    if (IsValidStoreState(store) && GetFetchStatus(store).ShouldAddToQueue()) {
        StoresQueue_.Push(store);
    }
}

void TCompactionHintFetcher::ClearQueue()
{
    StoresQueue_.Clear();
}

void TCompactionHintFetcher::OnRequestFailed(
    const IStorePtr& store,
    int requestStep)
{
    auto& status = GetFetchStatus(store);
    if (status.RequestStep == requestStep) {
        status.RequestStep = 0;
        AddToQueue(store);
    }
    FailedRequestCount_.Increment();
}

bool TCompactionHintFetcher::IsValidStoreState(const IStorePtr& store)
{
    return store->GetStoreState() == EStoreState::Persistent;
}

void TCompactionHintFetcher::GetStoresFromQueueAndMakeRequest()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (StoresQueue_.Empty()) {
        return;
    }

    if (i64 requestSize = RequestThrottler_->TryAcquireAvailable(StoresQueue_.Size()); requestSize > 0) {
        std::vector<IStorePtr> stores;
        while (ssize(stores) < requestSize && !StoresQueue_.Empty()) {
            if (auto store = StoresQueue_.PopAndGet();
                store && IsValidStoreState(store) && GetFetchStatus(store).ShouldMakeFirstRequest())
            {
                stores.push_back(std::move(store));
            }
        }

        if (!stores.empty()) {
            RequestCount_.Increment(ssize(stores));
            MakeRequest(std::move(stores));
        }
    } else {
        ThrottledRequestCount_.Increment();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
