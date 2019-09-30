#pragma once

#include "public.h"

#include <yp/server/objects/public.h>

#include <yp/server/net/public.h>

#include <yp/server/nodes/public.h>

#include <yp/server/scheduler/public.h>

#include <yp/server/access_control/public.h>

#include <yp/server/accounting/public.h>

#include <yt/client/node_tracker_client/public.h>

#include <yt/ytlib/auth/public.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/actions/public.h>

namespace NYP::NServer::NMaster {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(TMasterConfigPtr config, NYT::NYTree::INodePtr configPatchNode);

    const IInvokerPtr& GetControlInvoker();
    const IInvokerPtr& GetWorkerPoolInvoker();
    const TYTConnectorPtr& GetYTConnector();
    const NObjects::TObjectManagerPtr& GetObjectManager();
    const NNet::TNetManagerPtr& GetNetManager();
    const NObjects::TTransactionManagerPtr& GetTransactionManager();
    const NObjects::TWatchManagerPtr& GetWatchManager();
    const NNodes::TNodeTrackerPtr& GetNodeTracker();
    const NScheduler::TSchedulerPtr& GetScheduler();
    const NScheduler::TResourceManagerPtr& GetResourceManager();
    const NAccessControl::TAccessControlManagerPtr& GetAccessControlManager();
    const NAccounting::TAccountingManagerPtr& GetAccountingManager();
    const NAuth::TAuthenticationManagerPtr& GetAuthenticationManager();
    const NAuth::ISecretVaultServicePtr& GetSecretVaultService();

    const TString& GetFqdn();
    const NYT::NNodeTrackerClient::TAddressMap & GetInternalRpcAddresses();
    const TString& GetClientGrpcAddress();
    const TString& GetSecureClientGrpcAddress();
    const TString& GetClientHttpAddress();
    const TString& GetSecureClientHttpAddress();
    const TString& GetAgentGrpcAddress();

    void Run();

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NMaster
