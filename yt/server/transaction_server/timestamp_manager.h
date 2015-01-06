#pragma once

#include "public.h"

#include <core/actions/public.h>

#include <core/rpc/public.h>

#include <server/hydra/public.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

class TTimestampManager
    : public TRefCounted
{
public:
    TTimestampManager(
        TTimestampManagerConfigPtr config,
        IInvokerPtr automatonInvoker,
        NHydra::IHydraManagerPtr hydraManager,
        NHydra::TCompositeAutomatonPtr automaton);

    ~TTimestampManager();

    NRpc::IServicePtr GetRpcService();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TTimestampManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
