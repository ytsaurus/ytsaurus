#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra/mutation.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/concurrency/public.h>

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

    virtual const TMasterAutomatonPtr& GetAutomaton() = 0;
    virtual const NElection::IElectionManagerPtr& GetElectionManager() = 0;
    virtual const NHydra::IHydraManagerPtr& GetHydraManager() = 0;
    virtual const NHydra::IPersistentResponseKeeperPtr& GetResponseKeeper() = 0;
    virtual const NHydra::ILocalHydraJanitorPtr& GetLocalJanitor() = 0;

    virtual IInvokerPtr GetAutomatonInvoker(EAutomatonThreadQueue queue) = 0;
    virtual IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue) = 0;
    virtual IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue) = 0;

    virtual IInvokerPtr GetTransactionTrackerInvoker() = 0;

    virtual NConcurrency::IThreadPoolPtr GetSnapshotSaveBackgroundThreadPool() = 0;
    virtual NConcurrency::IThreadPoolPtr GetSnapshotLoadBackgroundThreadPool() = 0;

    virtual void BlockAutomaton() = 0;
    virtual void UnblockAutomaton() = 0;

    virtual bool IsAutomatonLocked() = 0;

    virtual void VerifyPersistentStateRead() = 0;

    //! Throws TLeaderFallbackException at followers.
    virtual void RequireLeader() = 0;

    virtual void Reconfigure(const TDynamicCellMasterConfigPtr& newConfig) = 0;

    virtual IInvokerPtr CreateEpochInvoker(IInvokerPtr underlyingInvoker) = 0;

    virtual const NObjectServer::TEpochContextPtr& GetEpochContext() = 0;

    virtual void CommitMutationWithSemaphore(
        const NConcurrency::TAsyncSemaphorePtr& semaphore,
        NRpc::IServiceContextPtr context,
        TCallback<std::unique_ptr<NHydra::TMutation>()> mutationBuilder,
        TCallback<void(const NHydra::TMutationResponse& response)> replyCallback = {}) = 0;
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
