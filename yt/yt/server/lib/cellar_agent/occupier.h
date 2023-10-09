#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/server/lib/transaction_supervisor/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

struct ICellarOccupier
    : public virtual TRefCounted
{
    virtual void SetOccupant(ICellarOccupantPtr occupant) = 0;

    virtual NHydra::TCompositeAutomatonPtr CreateAutomaton() = 0;

    virtual void Configure(NHydra::IDistributedHydraManagerPtr hydraManager) = 0;

    virtual NTransactionSupervisor::ITransactionManagerPtr GetOccupierTransactionManager() = 0;

    virtual void Initialize() = 0;
    virtual void RegisterRpcServices() = 0;

    virtual IInvokerPtr GetOccupierAutomatonInvoker() = 0;
    virtual IInvokerPtr GetMutationAutomatonInvoker() = 0;

    virtual NYTree::TCompositeMapServicePtr PopulateOrchidService(
        NYTree::TCompositeMapServicePtr orchidService) = 0;

    virtual void Stop() = 0;
    virtual void Finalize() = 0;

    virtual NCellarClient::ECellarType GetCellarType() = 0;

    virtual NProfiling::TProfiler GetProfiler() = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellarOccupier)

////////////////////////////////////////////////////////////////////////////////

struct ICellarOccupierProvider
    : public TRefCounted
{
    virtual ICellarOccupierPtr CreateCellarOccupier(int index) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellarOccupierProvider)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
