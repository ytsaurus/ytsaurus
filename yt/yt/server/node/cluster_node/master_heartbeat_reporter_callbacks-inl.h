#ifndef MASTER_HEARTBEAT_REPORTER_CALLBACKS_H_
#error "Direct inclusion of this file is not allowed, include master_heartbeat_reporter.h"
// For the sake of sane code completion.
#include "master_heartbeat_reporter_callbacks.h"
#endif

namespace NYT::NClusterNode {

////////////////////////////////////////////////////////////////////////////////

template <class TMasterConnector, class TNodeServiceProxy>
class TSingleFlavorHeartbeatCallbacks
    : public IMasterHeartbeatReporterCallbacks
{
public:
    TSingleFlavorHeartbeatCallbacks(
        TWeakPtr<TMasterConnector> owner,
        const NLogging::TLogger& logger)
        : Owner_(std::move(owner))
        , Logger(logger)
    { }

    TFuture<void> ReportHeartbeat(NObjectClient::TCellTag cellTag) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto owner = Owner_.Lock();
        if (!owner) {
            return MakeFuture(TError("Master connector is destroyed"));
        }

        auto req = owner->BuildHeartbeatRequest(cellTag);

        YT_LOG_INFO("Sending node heartbeat to master (CellTag: %v)",
            cellTag);

        auto future = req->Invoke();
        EmplaceOrCrash(CellTagToFuture_, cellTag, future);
        return future.AsVoid();
    }

    void OnHeartbeatSucceeded(NObjectClient::TCellTag cellTag) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto owner = Owner_.Lock();
        if (!owner) {
            return;
        }

        auto rspOrError = GetResponseOrError(cellTag);
        YT_VERIFY(rspOrError.IsOK());
        const auto& response = rspOrError.Value();

        owner->OnHeartbeatSucceeded(cellTag, response);
    }

    void OnHeartbeatFailed(NObjectClient::TCellTag cellTag) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto rspOrError = GetResponseOrError(cellTag);
        YT_VERIFY(!rspOrError.IsOK());

        YT_LOG_WARNING(rspOrError, "Error reporting node heartbeat to master (CellTag: %v)",
            cellTag);
    }

    void Reset(NObjectClient::TCellTag cellTag) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        CellTagToFuture_.erase(cellTag);
    }

private:
    using TNodeRspHeartbeat = TNodeServiceProxy::TRspHeartbeatPtr;

    const TWeakPtr<TMasterConnector> Owner_;
    THashMap<NObjectClient::TCellTag, TFuture<TNodeRspHeartbeat>> CellTagToFuture_;

    const NLogging::TLogger Logger;

    TErrorOr<TNodeRspHeartbeat> GetResponseOrError(NObjectClient::TCellTag cellTag)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto futureIt = GetIteratorOrCrash(CellTagToFuture_, cellTag);
        auto future = std::move(futureIt->second);
        CellTagToFuture_.erase(futureIt);

        // Future should be awaited in master heartbeat reporter.
        YT_VERIFY(future.IsSet());
        return future.Get();
    }

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

template <class TMasterConnector, class TNodeServiceProxy>
IMasterHeartbeatReporterCallbacksPtr CreateSingleFlavorHeartbeatCallbacks(
    TWeakPtr<TMasterConnector> owner,
    const NLogging::TLogger& logger)
{
    return New<TSingleFlavorHeartbeatCallbacks<TMasterConnector, TNodeServiceProxy>>(
        std::move(owner),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
