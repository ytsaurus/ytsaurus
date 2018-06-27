#pragma once

#include "public.h"

#include <yp/server/objects/public.h>

#include <yp/server/net/public.h>

#include <yp/server/nodes/public.h>

#include <yp/server/scheduler/public.h>

#include <yp/server/access_control/public.h>

#include <yt/ytlib/auth/public.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/actions/public.h>

namespace NYP {
namespace NServer {
namespace NMaster {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    explicit TBootstrap(TMasterConfigPtr config);

    const IInvokerPtr& GetControlInvoker();
    const IInvokerPtr& GetWorkerPoolInvoker();
    const TYTConnectorPtr& GetYTConnector();
    const NObjects::TObjectManagerPtr& GetObjectManager();
    const NNet::TNetManagerPtr& GetNetManager();
    const NObjects::TTransactionManagerPtr& GetTransactionManager();
    const NNodes::TNodeTrackerPtr& GetNodeTracker();
    const NScheduler::TResourceManagerPtr& GetResourceManager();
    const NAccessControl::TAccessControlManagerPtr& GetAccessControlManager();
    const NAuth::TAuthenticationManagerPtr& GetAuthenticationManager();

    const TString& GetFqdn();
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

} // namespace NMaster
} // namespace NServer
} // namespace NYP
