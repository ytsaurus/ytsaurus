#pragma once

#include "public.h"

#include <yt/server/lib/hydra/public.h>

#include <yt/library/profiling/sensor.h>

#include <yt/core/actions/public.h>

#include <yt/core/ytree/public.h>

#include <yt/core/rpc/public.h>

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

struct ICellarOccupier
    : public TRefCounted
{
    virtual void SetOccupant(ICellarOccupantPtr occupant) = 0;

    virtual NHydra::TCompositeAutomatonPtr CreateAutomaton() = 0;

    virtual void Configure(NHydra::IDistributedHydraManagerPtr hydraManager) = 0;

    virtual const NHiveServer::ITransactionManagerPtr GetOccupierTransactionManager() = 0;

    virtual void Initialize() = 0;
    virtual void RegisterRpcServices() = 0;

    virtual IInvokerPtr GetOccupierAutomatonInvoker() = 0;
    virtual IInvokerPtr GetMutationAutomatonInvoker() = 0;

    virtual NYTree::TCompositeMapServicePtr PopulateOrchidService(
        NYTree::TCompositeMapServicePtr orchidService) = 0;

    virtual void Stop() = 0;
    virtual void Finalize() = 0;

    virtual ECellarType GetCellarType() = 0;

    virtual NProfiling::TRegistry GetProfiler() = 0;
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
