#include "global_stores_update_throttler.h"

#include "config.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/tablet_client/master_tablet_service_proxy.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TGlobalStoresUpdateThrottler::TGlobalStoresUpdateThrottler(
    TGlobalStoresUpdateThrottlerConfigPtr config,
    NNative::IConnectionPtr connection,
    const TProfiler& profiler)
    : Config_(std::move(config))
    , Connection_(std::move(connection))
    , Profiler_(profiler)
    , FailedThrottleRequestCounter_(profiler.Counter("/failed_throttle_requests"))
{ }

void TGlobalStoresUpdateThrottler::Reconfigure(TGlobalStoresUpdateThrottlerConfigPtr newConfig)
{
    Config_.Store(std::move(newConfig));
}

bool TGlobalStoresUpdateThrottler::IsEnabled() const
{
    auto config = Config_.Acquire();
    return config && config->Enable;
}

TFuture<void> TGlobalStoresUpdateThrottler::InvokeMasterRequest(
    TCellStatus* cellStatus,
    TCellTag cellTag,
    ETabletStoresUpdateReason updateReason)
{
    if (auto channel = Connection_->FindMasterChannel(EMasterChannelKind::Leader, cellTag)) {
        TMasterTabletServiceProxy proxy(std::move(channel));
        auto req = proxy.ThrottleTabletStoresUpdate();
        req->SetTimeout(Config_.Acquire()->RpcTimeout);

        if (!cellStatus->BundleName.empty()) {
            req->set_bundle_name(cellStatus->BundleName);
        }
        req->set_update_reason(ToProto(updateReason));
        ToProto(req->mutable_store_counts(), cellStatus->RequestedStoreCounts);

        cellStatus->ScheduledRequestFuture = req->Invoke().Apply(
            BIND([] (const TMasterTabletServiceProxy::TRspThrottleTabletStoresUpdatePtr& rsp) {
                return rsp->accepted_request_count();
            }));
    } else {
        cellStatus->ScheduledRequestFuture = MakeFuture<int>(TError("Unknown cell tag %v", cellTag));
    }

    return cellStatus->ScheduledRequestFuture.AsVoid();
}

std::vector<bool> TGlobalStoresUpdateThrottler::Throttle(
    const std::vector<TRequest>& requests,
    ETabletStoresUpdateReason updateReason)
{
    if (requests.empty()) {
        return {};
    }

    if (!IsEnabled()) {
        return std::vector<bool>(requests.size(), true);
    }

    // COMPAT(alexelexa)
    if (LastNoSuchMethodError_.load() + Config_.Acquire()->NoSuchMethodBackoffTime > TInstant::Now()) {
        return std::vector<bool>(requests.size(), true);
    }

    THashMap<TCellTag, TCellStatus> cellStatuses;
    for (int index = 0; index < ssize(requests); ++index) {
        const auto& request = requests[index];
        auto& cellStatus = cellStatuses[request.CellTag];

        if (cellStatus.RequestedStoreCounts.empty()) {
            cellStatus.BundleName = request.BundleName;
        } else if (cellStatus.BundleName != request.BundleName) {
            // Master can throttle by bundle only if the whole request comes from one bundle; a mixed
            // batch is unlikely and thus unsupported, so drop the bundle name.
            cellStatus.BundleName.clear();
        }

        cellStatus.RequestedStoreCounts.push_back(request.StoreCount);
        cellStatus.RequestIndexes.push_back(index);
    }

    std::vector<TFuture<void>> futures;
    futures.reserve(cellStatuses.size());
    for (auto& [cellTag, cellStatus] : cellStatuses) {
        YT_TLOG_DEBUG("Sending tablet stores update throttling request")
            .With("CellTag", cellTag)
            .With("BundleName", cellStatus.BundleName)
            .With("UpdateReason", updateReason)
            .With("StoreCounts", cellStatus.RequestedStoreCounts);
        futures.push_back(InvokeMasterRequest(&cellStatus, cellTag, updateReason));
    }

    std::vector<bool> responses(requests.size(), false);

    if (auto error = WaitFor(AllSet(std::move(futures))); !error.IsOK()) {
        YT_TLOG_ERROR("Failed to throttle tablet stores update")
            .With(error);
        FailedThrottleRequestCounter_.Increment(ssize(cellStatuses));
        return responses;
    }

    for (const auto& [cellTag, cellStatus] : cellStatuses) {
        auto requestCount = ssize(cellStatus.RequestIndexes);
        const auto& acceptedRequestCountOrError = cellStatus.ScheduledRequestFuture.GetOrCrash();

        i64 acceptedRequestCount = 0;
        if (acceptedRequestCountOrError.IsOK()) {
            acceptedRequestCount = acceptedRequestCountOrError.Value();
        } else if (acceptedRequestCountOrError.FindMatching(NRpc::EErrorCode::NoSuchMethod)) {
            YT_TLOG_WARNING("Failed to throttle tablet stores update, remembering \"No such method\" error")
                .With("CellTag", cellTag)
                .With(acceptedRequestCountOrError);

            LastNoSuchMethodError_.store(TInstant::Now());
            acceptedRequestCount = requestCount;
        } else {
            YT_TLOG_WARNING("Failed to throttle tablet stores update")
                .With("CellTag", cellTag)
                .With(acceptedRequestCountOrError);
            FailedThrottleRequestCounter_.Increment();
        }

        YT_TLOG_DEBUG("Finished throttling tablet stores update at master")
            .With("CellTag", cellTag)
            .With("RequestCount", requestCount)
            .With("Result", acceptedRequestCount);
        acceptedRequestCount = std::clamp<i64>(acceptedRequestCount, 0, requestCount);

        if (acceptedRequestCount < requestCount) {
            GetOrCreateThrottledCounter(cellTag).Increment();
        }

        for (i64 subrequestIndex = 0; subrequestIndex < acceptedRequestCount; ++subrequestIndex) {
            responses[cellStatus.RequestIndexes[subrequestIndex]] = true;
        }
    }

    return responses;
}

TCounter& TGlobalStoresUpdateThrottler::GetOrCreateThrottledCounter(TCellTag cellTag)
{
    auto guard = Guard(CounterLock_);

    if (auto it = ThrottledRequestCounters_.find(cellTag); it != ThrottledRequestCounters_.end()) {
        return it->second;
    }

    return EmplaceOrCrash(
        ThrottledRequestCounters_,
        cellTag,
        Profiler_
            .WithSparse()
            .WithTag("cell_tag", ToString(cellTag))
            .Counter("/throttled_update_tablet_stores"))->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
