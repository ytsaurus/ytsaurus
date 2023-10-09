#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NTimestampServer {

////////////////////////////////////////////////////////////////////////////////

class TTimestampManager
    : public TRefCounted
{
public:
    TTimestampManager(
        TTimestampManagerConfigPtr config,
        IInvokerPtr automatonInvoker,
        NHydra::IHydraManagerPtr hydraManager,
        NHydra::TCompositeAutomatonPtr automaton,
        NObjectClient::TCellTag cellTag,
        NRpc::IAuthenticatorPtr authenticator);

    ~TTimestampManager();

    NRpc::IServicePtr GetRpcService();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TTimestampManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTimestampServer
