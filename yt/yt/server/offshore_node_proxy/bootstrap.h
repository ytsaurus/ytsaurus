#pragma once

#include "private.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/library/monitoring/http_integration.h>
#include <yt/yt/library/monitoring/monitoring_manager.h>

#include <yt/yt/core/http/server.h>

namespace NYT::NOffshoreNodeProxy {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(TOffshoreNodeProxyProgramConfigPtr config, NYTree::INodePtr configNode);
    ~TBootstrap();

    void Run();

private:
    const TOffshoreNodeProxyProgramConfigPtr Config_;
    const NYTree::INodePtr ConfigNode_;

    const NConcurrency::TActionQueuePtr ControlQueue_;
    const IInvokerPtr ControlInvoker_;
    const TOffshoreNodeProxyDynamicConfigPtr DynamicConfig_;

    const NConcurrency::IThreadPoolPtr StorageThreadPool_;

    TString InstanceId_;

    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    NYT::NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;
    NCoreDump::ICoreDumperPtr CoreDumper_;

    NApi::NNative::IConnectionPtr NativeConnection_;
    NApi::NNative::IClientPtr NativeClient_;

    NRpc::IAuthenticatorPtr NativeAuthenticator_;

    TDynamicConfigManagerPtr DynamicConfigManager_;

    NAlertManager::IAlertManagerPtr AlertManager_;

    void DoRun();

    //! Creates instance node with proper annotations and an orchid node at the native cluster.
    void UpdateCypressNode();

    void OnDynamicConfigChanged(
        const TOffshoreNodeProxyDynamicConfigPtr& oldConfig,
        const TOffshoreNodeProxyDynamicConfigPtr& newConfig);

    void CreateStateTablesIfNeeded();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreNodeProxy
