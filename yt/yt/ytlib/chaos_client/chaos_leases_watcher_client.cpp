#include "chaos_leases_watcher_client.h"

#include "chaos_node_service_proxy.h"
#include "chaos_residency_cache.h"
#include "master_cache_channel.h"
#include "object_watcher_client.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NChaosClient {

using namespace NApi::NNative;
using namespace NLogging;
using namespace NObjectClient;
using namespace NRpc;
using namespace NTracing;
using namespace NTransactionClient;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ChaosLeaseWatcherClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TChaosLeasesWatcherClient
    : public IChaosLeasesWatcherClient
    , public TObjectWatcherClientBase
{
public:
    TChaosLeasesWatcherClient(
        std::unique_ptr<IChaosLeaseWatcherClientCallbacks> callbacks,
        TWeakPtr<IConnection> connection)
        : TObjectWatcherClientBase(std::move(connection))
        , Callbacks_(std::move(callbacks))
    { }

    TChaosLeasesWatcherClient(
        std::unique_ptr<IChaosLeaseWatcherClientCallbacks> callbacks,
        IChannelPtr chaosCacheChannel,
        TWeakPtr<IConnection> connection)
        : TObjectWatcherClientBase(std::move(connection), std::move(chaosCacheChannel))
        , Callbacks_(std::move(callbacks))
    { }

    void WatchChaosLease(TChaosLeaseId chaosLeaseId) override
    {
        WatchObject(chaosLeaseId);
    }

    void StopWatchingChaosLease(TChaosLeaseId chaosLeaseId) override
    {
        if (!StopWatchingObject(chaosLeaseId)) {
            return;
        }

        YT_TLOG_DEBUG("Stopped watching chaos lease")
            .With("ChaosLeaseId", chaosLeaseId);
    }

private:
    const std::unique_ptr<IChaosLeaseWatcherClientCallbacks> Callbacks_;

    TFuture<void> WatchUpstream(TChaosLeaseId chaosLeaseId, TTimestamp timestamp) override
    {
        auto connection = GetConnection();
        if (!connection) {
            return MakeFuture(TError("Connection is not available"));
        }

        auto channel = GetWatchChannel(connection, chaosLeaseId);

        auto proxy = TChaosNodeServiceProxy(std::move(channel));
        proxy.SetDefaultTimeout(connection->GetConfig()->DefaultChaosWatcherClientRequestTimeout);

        auto req = proxy.WatchChaosLease();
        ToProto(req->mutable_chaos_lease_id(), chaosLeaseId);
        req->set_chaos_lease_cache_timestamp(ToProto(timestamp));

        SetChaosCacheStickyGroupBalancingHint(
            chaosLeaseId,
            req->Header().MutableExtension(NRpc::NProto::TBalancingExt::balancing_ext));

        auto traceContext = TTraceContext::NewRoot("ChaosLeaseWatcherClient");
        TTraceContextGuard traceContextGuard(traceContext);

        return req->Invoke().AsUnique().Apply(
            BIND(
                &TChaosLeasesWatcherClient::OnChaosLeaseWatchResponse,
                MakeStrong(this),
                chaosLeaseId)
            .AsyncVia(GetCurrentInvoker()));
    }

    void OnChaosLeaseWatchResponse(
        TChaosLeaseId chaosLeaseId,
        TErrorOr<TChaosNodeServiceProxy::TRspWatchChaosLeasePtr>&& response)
    {
        if (!response.IsOK()) {
            RemoveWatch(chaosLeaseId);
            YT_TLOG_DEBUG("Error watching chaos lease")
                .With("ChaosLeaseId", chaosLeaseId)
                .With(response);
            return;
        }

        const auto& value = response.Value();
        if (value->has_chaos_lease_deleted()) {
            RemoveWatch(chaosLeaseId);
            Callbacks_->OnChaosLeaseDeleted(chaosLeaseId);
            return;
        }

        if (value->has_unknown_chaos_lease()) {
            RemoveWatch(chaosLeaseId);
            YT_TLOG_DEBUG("Unknown chaos lease")
                .With("ChaosLeaseId", chaosLeaseId)
                .With("Response", response);
            Callbacks_->OnUnknownChaosLease(chaosLeaseId);
            return;
        }

        auto connection = GetConnection();
        auto residencyCache = connection ? connection->GetChaosResidencyCache() : nullptr;

        if (value->has_chaos_lease_changed()) {
            const auto& changedResponse = value->chaos_lease_changed();
            auto responseTimestamp = FromProto<TTimestamp>(changedResponse.chaos_lease_cache_timestamp());

            auto chaosLease = New<TChaosLease>();
            chaosLease->Timeout = FromProto<TDuration>(changedResponse.timeout());
            FromProto(&chaosLease->CoordinatorCellIds, changedResponse.coordinator_cell_ids());

            if (residencyCache) {
                residencyCache->PingChaosObjectResidency(chaosLeaseId);
            }

            YT_TLOG_DEBUG("Chaos lease changed")
                .With("ChaosLeaseId", chaosLeaseId)
                .With("Response", response);

            Callbacks_->OnChaosLeaseUpdated(chaosLeaseId, std::move(chaosLease), responseTimestamp);

            if (!RearmWatch(chaosLeaseId, responseTimestamp)) {
                YT_TLOG_DEBUG("Changed response received but chaos lease was already removed from cache")
                    .With("ChaosLeaseId", chaosLeaseId);
            }
            return;
        }

        if (value->has_chaos_lease_not_changed()) {
            if (residencyCache) {
                residencyCache->PingChaosObjectResidency(chaosLeaseId);
            }

            YT_TLOG_DEBUG("Chaos lease not changed")
                .With("ChaosLeaseId", chaosLeaseId)
                .With("Response", response);

            Callbacks_->OnNothingChanged(chaosLeaseId);

            if (!RearmWatch(chaosLeaseId)) {
                YT_TLOG_DEBUG("Nothing changed response received but chaos lease was already removed from cache")
                    .With("ChaosLeaseId", chaosLeaseId);
            }
            return;
        }

        if (value->has_chaos_lease_migrated()) {
            const auto& migratedResponse = value->chaos_lease_migrated();
            auto newCellId = FromProto<TCellId>(migratedResponse.migrate_to_cell_id());
            auto newCellTag = CellTagFromId(newCellId);
            if (residencyCache) {
                residencyCache->UpdateChaosObjectResidency(chaosLeaseId, newCellTag);
            }

            YT_TLOG_DEBUG("Chaos lease migrated")
                .With("ChaosLeaseId", chaosLeaseId)
                .With("Response", response);
            Callbacks_->OnChaosLeaseMigrated(chaosLeaseId);

            if (!RearmWatch(chaosLeaseId)) {
                YT_TLOG_DEBUG("Migrated response received but chaos lease was already removed from cache")
                    .With("ChaosLeaseId", chaosLeaseId);
            }
            return;
        }

        if (value->has_instance_is_not_leader()) {
            if (residencyCache) {
                residencyCache->RemoveChaosObjectResidency(chaosLeaseId);
            }

            YT_TLOG_DEBUG("Instance is not leader for chaos lease")
                .With("ChaosLeaseId", chaosLeaseId)
                .With("Response", response);

            Callbacks_->OnNothingChanged(chaosLeaseId);

            if (!RearmWatch(chaosLeaseId)) {
                YT_TLOG_DEBUG("Leader switch response received but chaos lease was already removed from cache")
                    .With("ChaosLeaseId", chaosLeaseId);
            }
            return;
        }
    }
};

IChaosLeasesWatcherClientPtr CreateChaosLeasesWatcherClient(
    std::unique_ptr<IChaosLeaseWatcherClientCallbacks> callbacks,
    TWeakPtr<IConnection> connection)
{
    return New<TChaosLeasesWatcherClient>(
        std::move(callbacks),
        std::move(connection));
}

IChaosLeasesWatcherClientPtr CreateChaosLeasesWatcherClient(
    std::unique_ptr<IChaosLeaseWatcherClientCallbacks> callbacks,
    IChannelPtr chaosCacheChannel,
    TWeakPtr<IConnection> connection)
{
    return New<TChaosLeasesWatcherClient>(
        std::move(callbacks),
        std::move(chaosCacheChannel),
        std::move(connection));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
