#pragma once

#include "public.h"

#include <yt/client/election/public.h>

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/server/lib/security_server/public.h>

#include <yt/core/actions/public.h>

#include <yt/core/rpc/public.h>

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

struct ICellarBootstrapProxy
    : public TRefCounted
{
    virtual NElection::TCellId GetCellId() const = 0;

    virtual NApi::NNative::IClientPtr GetMasterClient() const = 0;

    virtual NNodeTrackerClient::TNetworkPreferenceList GetLocalNetworks() const = 0;

    virtual IInvokerPtr GetControlInvoker() const = 0;
    virtual IInvokerPtr GetTransactionTrackerInvoker() const = 0;

    virtual NRpc::IServerPtr GetRpcServer() const = 0;

    virtual NSecurityServer::IResourceLimitsManagerPtr GetResourceLimitsManager() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellarBootstrapProxy)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
