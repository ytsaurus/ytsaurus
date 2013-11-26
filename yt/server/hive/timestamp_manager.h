#pragma once

#include "public.h"

#include <core/actions/invoker.h>

#include <core/rpc/public.h>

#include <server/hydra/public.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

class TTimestampManager
    : public TRefCounted
{
public:
    TTimestampManager(
        TTimestampManagerConfigPtr config,
        IInvokerPtr automatonInvoker,
        NRpc::IRpcServerPtr rpcServer,
        NHydra::IHydraManagerPtr hydraManager,
        NHydra::TCompositeAutomatonPtr automaton);

    ~TTimestampManager();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
