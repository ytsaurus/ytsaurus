#pragma once

#include "private.h"

#include <yt/yt/server/lib/cypress_election/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/discovery_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/library/monitoring/public.h>

#include <yt/yt/library/coredumper/public.h>

#include <yt/yt/core/bus/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(TQueueAgentServerConfigPtr config, NYTree::INodePtr configNode);
    ~TBootstrap();

    void Run();

private:
    const TQueueAgentServerConfigPtr Config_;
    const NYTree::INodePtr ConfigNode_;
    const TQueueAgentServerDynamicConfigPtr DynamicConfig_;

    TString AgentId_;
    TString GroupId_;

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

    NHiveClient::TClientDirectoryPtr ClientDirectory_;

    NDiscoveryClient::IMemberClientPtr MemberClient_;
    NDiscoveryClient::IDiscoveryClientPtr DiscoveryClient_;
    NCypressElection::ICypressElectionManagerPtr ElectionManager_;

    NAlertManager::IAlertManagerPtr AlertManager_;

    NQueueClient::TDynamicStatePtr DynamicState_;

    IQueueAgentShardingManagerPtr QueueAgentShardingManager_;
    TQueueAgentPtr QueueAgent_;

    ICypressSynchronizerPtr CypressSynchronizer_;

    void DoRun();

    //! Creates instance node with proper annotations and an orchid node at the native cluster.
    void UpdateCypressNode();

    void OnDynamicConfigChanged(
        const TQueueAgentServerDynamicConfigPtr& oldConfig,
        const TQueueAgentServerDynamicConfigPtr& newConfig);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
