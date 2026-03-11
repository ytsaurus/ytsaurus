#include "bundle_controller_connector.h"

#include "bootstrap.h"
#include "config.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/bundle_controller/bundle_controller_service_proxy.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt_proto/yt/client/bundle_controller/proto/bundle_controller_service.pb.h>

#include <yt/yt/core/concurrency/retrying_periodic_executor.h>

namespace NYT::NCellarNode {

////////////////////////////////////////////////////////////////////////////////

using namespace NBundleController;
using namespace NConcurrency;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = CellarNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TBundleControllerConnector::TBundleControllerConnector(IBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , DynamicConfig_(New<TBundleControllerConnectorDynamicConfig>())
    , HeartbeatExecutor_(New<TRetryingPeriodicExecutor>(
        Bootstrap_->GetControlInvoker(),
        BIND([this, weakThis = MakeWeak(this)] {
            auto this_ = weakThis.Lock();
            return this_ ? SendHeartbeat() : TError("Bundle controller connector is destroyed");
        }),
        DynamicConfig_->HeartbeatExecutor))
{
    YT_ASSERT_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());
}

void TBundleControllerConnector::Initialize()
{ }

void TBundleControllerConnector::OnDynamicConfigChanged(
    const TBundleControllerConnectorDynamicConfigPtr& oldConfig,
    const TBundleControllerConnectorDynamicConfigPtr& newConfig)
{
    DynamicConfig_ = newConfig;

    YT_LOG_DEBUG(
        "Set new bundle controller heartbeat options "
        "(NewPeriod: %v, NewSplay: %v, NewJitter: %v, "
        "NewMinBackoff: %v, NewMaxBackoff: %v, NewBackoffMultiplier: %v, "
        "NewHeartbeatTimeout: %v)",
        newConfig->HeartbeatExecutor.Period,
        newConfig->HeartbeatExecutor.Splay,
        newConfig->HeartbeatExecutor.Jitter,
        newConfig->HeartbeatExecutor.MinBackoff,
        newConfig->HeartbeatExecutor.MaxBackoff,
        newConfig->HeartbeatExecutor.BackoffMultiplier,
        newConfig->HeartbeatTimeout);

    HeartbeatExecutor_->SetOptions(newConfig->HeartbeatExecutor);

    if (!oldConfig->Enable && newConfig->Enable) {
        HeartbeatExecutor_->Start();
        YT_LOG_INFO("Started bundle controller connector after changing the config");
    }

    if (oldConfig->Enable && !newConfig->Enable) {
        YT_UNUSED_FUTURE(HeartbeatExecutor_->Stop());
        YT_LOG_INFO("Stopped bundle controller connector after changing the config");
    }
}

TError TBundleControllerConnector::SendHeartbeat()
{
    YT_ASSERT_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

    if (!Bootstrap_->IsConnected()) {
        return TError("Node is not connected");
    }

    const auto& client = Bootstrap_->GetClient();
    TBundleControllerServiceProxy proxy(client->GetNativeConnection()->GetBundleControllerChannel());

    auto req = proxy.Heartbeat();
    PrepareHeartbeatRequest(req);

    YT_LOG_INFO("Sending node heartbeat to bundle controller");

    auto rspOrError = WaitFor(req->Invoke());
    if (!rspOrError.IsOK()) {
        auto [minBackoff, maxBackoff] = HeartbeatExecutor_->GetBackoffInterval();
        YT_LOG_ERROR(
            rspOrError,
            "Failed to report heartbeat to bundle controller (BackoffTime: [%v, %v])",
            minBackoff,
            maxBackoff);
        return TError("Failed to report heartbeat to bundle controller");
    }

    YT_LOG_INFO("Successfully reported heartbeat to bundle controller");

    return {};
}

void TBundleControllerConnector::PrepareHeartbeatRequest(const TReqClientHeartbeatPtr& request)
{
    request->SetTimeout(DynamicConfig_->HeartbeatTimeout);

    request->set_node_id(ToProto(Bootstrap_->GetNodeId()));
    request->set_node_address(ToProto(
        Bootstrap_->GetLocalDescriptor().GetDefaultAddress()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarNode
