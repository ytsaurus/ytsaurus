#pragma once

#include "private.h"

#include <yt/yt/server/lib/cypress_election/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/library/monitoring/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/library/coredumper/public.h>

#include <yt/yt/core/bus/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(TQueryTrackerServerConfigPtr config, NYTree::INodePtr configNode);
    ~TBootstrap();

    void Run();

private:
    const TQueryTrackerServerConfigPtr Config_;
    const NYTree::INodePtr ConfigNode_;

    TString SelfAddress_;

    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    NConcurrency::TActionQueuePtr ControlQueue_;
    IInvokerPtr ControlInvoker_;
    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;
    NCoreDump::ICoreDumperPtr CoreDumper_;

    NApi::NNative::IConnectionPtr NativeConnection_;
    NApi::NNative::IClientPtr NativeClient_;

    NRpc::IAuthenticatorPtr NativeAuthenticator_;

    TDynamicConfigManagerPtr DynamicConfigManager_;

    TAlertManagerPtr AlertManager_;

    IQueryTrackerPtr QueryTracker_;

    void DoRun();

    //! Creates instance node with proper annotations and an orchid node at the native cluster.
    void UpdateCypressNode();

    void OnDynamicConfigChanged(
        const TQueryTrackerServerDynamicConfigPtr& oldConfig,
        const TQueryTrackerServerDynamicConfigPtr& newConfig);

    void CreateStateTablesIfNeeded();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
