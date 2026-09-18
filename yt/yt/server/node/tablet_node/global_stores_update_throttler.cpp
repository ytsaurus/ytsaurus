#include "global_stores_update_throttler.h"

#include "config.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/tablet_client/master_tablet_service_proxy.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/misc/finally.h>

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

void TGlobalStoresUpdateThrottler::AddRequest(const std::string& bundleName, int storeCount, TCellTag cellTag)
{
    if (!IsEnabled()) {
        Responses_.push_back(true);
        return;
    }

    // COMPAT(alexelexa)
    if (LastNoSuchMethodError_ + Config_.Acquire()->NoSuchMethodBackoffTime > TInstant::Now()) {
        Responses_.push_back(true);
        return;
    }

    auto& cellStatus = CellStatuses_[cellTag];
    if (cellStatus.RequestedStoreCounts.empty()) {
        cellStatus.BundleName = bundleName;
    } else if (cellStatus.BundleName != bundleName) {
        // Master can throttle by bundle only if the whole request comes from one bundle; a mixed
        // batch is unlikely and thus unsupported, so drop the bundle name.
        cellStatus.BundleName.clear();
    }

    cellStatus.RequestedStoreCounts.push_back(storeCount);
    cellStatus.RequestIndexes.push_back(ssize(Responses_));
    Responses_.push_back(false);
}

void TGlobalStoresUpdateThrottler::Reconfigure(TGlobalStoresUpdateThrottlerConfigPtr newConfig)
{
    Config_.Store(std::move(newConfig));
}

bool TGlobalStoresUpdateThrottler::IsEnabled() const
{
    auto config = Config_.Acquire();
    return config && config->Enable;
}

std::vector<bool> TGlobalStoresUpdateThrottler::Throttle(ETabletStoresUpdateReason updateReason)
{
    auto doneGuard = Finally([&] {
        Responses_.clear();
        CellStatuses_.clear();
    });

    DoThrottle(updateReason);
    return Responses_;
}

TFuture<void> TGlobalStoresUpdateThrottler::InvokeMasterRequest(
    TCellTag cellTag,
    TCellStatus* cellStatus,
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

void TGlobalStoresUpdateThrottler::DoThrottle(ETabletStoresUpdateReason updateReason)
{
    if (CellStatuses_.empty()) {
        return;
    }

    std::vector<TFuture<void>> futures;
    futures.reserve(CellStatuses_.size());

    for (auto& [cellTag, cellStatus] : CellStatuses_) {
        YT_TLOG_DEBUG("Sending tablet stores update throttling request")
            .With("CellTag", cellTag)
            .With("BundleName", cellStatus.BundleName)
            .With("UpdateReason", updateReason)
            .With("StoreCounts", cellStatus.RequestedStoreCounts);
        futures.push_back(InvokeMasterRequest(cellTag, &cellStatus, updateReason));
    }

    if (auto error = WaitFor(AllSet(std::move(futures))); !error.IsOK()) {
        YT_TLOG_ERROR("Failed to throttle tablet stores update")
            .With(error);
        FailedThrottleRequestCounter_.Increment(ssize(CellStatuses_));
        return;
    }

    for (const auto& [cellTag, cellStatus] : CellStatuses_) {
        auto requestCount = ssize(cellStatus.RequestIndexes);
        const auto& acceptedRequestCountOrError = cellStatus.ScheduledRequestFuture.GetOrCrash();

        i64 acceptedRequestCount = 0;
        if (acceptedRequestCountOrError.IsOK()) {
            acceptedRequestCount = acceptedRequestCountOrError.Value();
        } else if (acceptedRequestCountOrError.FindMatching(NRpc::EErrorCode::NoSuchMethod)) {
            YT_TLOG_WARNING("Failed to throttle tablet stores update, remembering \"No such method\" error")
                .With("CellTag", cellTag)
                .With(acceptedRequestCountOrError);

            LastNoSuchMethodError_ = TInstant::Now();
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
            Responses_[cellStatus.RequestIndexes[subrequestIndex]] = true;
        }
    }
}

TCounter& TGlobalStoresUpdateThrottler::GetOrCreateThrottledCounter(TCellTag cellTag)
{
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
