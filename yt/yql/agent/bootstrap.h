#pragma once

#include "private.h"

#include <yt/yt/server/lib/cypress_election/public.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/lib/component_state_checker/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/library/monitoring/public.h>
#include <yt/yt/library/server_program/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/library/coredumper/public.h>

#include <yt/yt/core/bus/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/public.h>


namespace NYT::NYqlAgent {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(TYqlAgentServerConfigPtr config, NYTree::INodePtr configNode);
    ~TBootstrap();

    void Run();
    
    const NServer::TNativeServerBootstrapConfigPtr GetNativeServerBootstrapConfig() const;

    const TServerProgramConfigPtr GetServerProgramConfig() const;

private:
    TAgentId AgentId_;

    const TYqlAgentServerConfigPtr Config_;
    const NYTree::INodePtr ConfigNode_;

    NMonitoring::IMonitoringManagerPtr MonitoringManager_;
    NConcurrency::TActionQueuePtr ControlQueue_;
    IInvokerPtr ControlInvoker_;
    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;
    NCoreDump::ICoreDumperPtr CoreDumper_;

    NComponentStateChecker::IComponentStateCheckerPtr ComponentStateChecker_;

    NApi::NNative::IConnectionPtr NativeConnection_;
    NApi::NNative::IClientPtr NativeClient_;

    TDynamicConfigManagerPtr DynamicConfigManager_;

    IYqlAgentPtr YqlAgent_;

    NRpc::IAuthenticatorPtr NativeAuthenticator_;

    void DoRun();

    //! Creates instance node with proper annotations and an orchid node at the native cluster.
    void UpdateCypressNode();
    void GuardedUpdateCypressNode();

    void OnDynamicConfigChanged(
        const TYqlAgentServerDynamicConfigPtr& oldConfig,
        const TYqlAgentServerDynamicConfigPtr& newConfig);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
