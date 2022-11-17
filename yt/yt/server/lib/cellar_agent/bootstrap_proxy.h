#pragma once

#include "public.h"

#include <yt/yt/client/election/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/server/lib/security_server/public.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

struct ICellarBootstrapProxy
    : public TRefCounted
{
    virtual NElection::TCellId GetCellId() const = 0;

    virtual NApi::NNative::IClientPtr GetClient() const = 0;

    virtual NNodeTrackerClient::TNetworkPreferenceList GetLocalNetworks() const = 0;

    virtual IInvokerPtr GetControlInvoker() const = 0;
    virtual IInvokerPtr GetTransactionTrackerInvoker() const = 0;

    virtual NRpc::IServerPtr GetRpcServer() const = 0;

    virtual NSecurityServer::IResourceLimitsManagerPtr GetResourceLimitsManager() const = 0;

    virtual void ScheduleCellarHeartbeat(bool immediately) const = 0;

    virtual NRpc::IAuthenticatorPtr GetNativeAuthenticator() const = 0;

    DECLARE_INTERFACE_SIGNAL(void(std::vector<TError>* alerts), PopulateAlerts);
};

DEFINE_REFCOUNTED_TYPE(ICellarBootstrapProxy)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
