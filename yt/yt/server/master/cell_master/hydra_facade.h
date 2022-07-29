#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra_common/mutation.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/test_framework/testing_tag.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TLeaderFallbackException
{ };

////////////////////////////////////////////////////////////////////////////////

struct IHydraFacade
    : public TRefCounted
{
public:
    virtual void Initialize() = 0;

    virtual const TMasterAutomatonPtr& GetAutomaton() const = 0;
    virtual const NElection::IElectionManagerPtr& GetElectionManager() const = 0;
    virtual const NHydra::IHydraManagerPtr& GetHydraManager() const = 0;
    virtual const NHydra::IPersistentResponseKeeperPtr& GetResponseKeeper() const = 0;

    virtual IInvokerPtr GetAutomatonInvoker(EAutomatonThreadQueue queue) const = 0;
    virtual IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue) const = 0;
    virtual IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue) const = 0;

    virtual IInvokerPtr GetTransactionTrackerInvoker() const = 0;

    virtual void BlockAutomaton() = 0;
    virtual void UnblockAutomaton() = 0;

    virtual bool IsAutomatonLocked() const = 0;

    virtual void VerifyPersistentStateRead() const = 0;

    //! Throws TLeaderFallbackException at followers.
    virtual void RequireLeader() const = 0;

    virtual void Reconfigure(const TDynamicCellMasterConfigPtr& newConfig) = 0;

    virtual IInvokerPtr CreateEpochInvoker(IInvokerPtr underlyingInvoker) const = 0;

    virtual const NObjectServer::TEpochContextPtr& GetEpochContext() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IHydraFacade)

////////////////////////////////////////////////////////////////////////////////

IHydraFacadePtr CreateHydraFacade(TBootstrap* bootstrap);
IHydraFacadePtr CreateHydraFacade(TTestingTag);

////////////////////////////////////////////////////////////////////////////////

class TAutomatonBlockGuard
{
public:
    TAutomatonBlockGuard(IHydraFacadePtr hydraFacade);

    ~TAutomatonBlockGuard();

private:
    const IHydraFacadePtr HydraFacade_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
