#pragma once

#include "public.h"

#include <yt/server/hydra/mutation.h>

#include <yt/ytlib/election/public.h>

#include <yt/core/rpc/public.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TLeaderFallbackException
{ };

////////////////////////////////////////////////////////////////////////////////

class THydraFacade
    : public TRefCounted
{
public:
    THydraFacade(
        TCellMasterConfigPtr config,
        TBootstrap* bootstrap);
    ~THydraFacade();

    void Initialize();
    void LoadSnapshot(NHydra::ISnapshotReaderPtr reader, bool dump);

    const TMasterAutomatonPtr& GetAutomaton() const;
    const NElection::IElectionManagerPtr& GetElectionManager() const;
    const NHydra::IHydraManagerPtr& GetHydraManager() const;
    const NRpc::TResponseKeeperPtr& GetResponseKeeper() const;

    IInvokerPtr GetAutomatonInvoker(EAutomatonThreadQueue queue) const;
    IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue) const;
    IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue) const;

    IInvokerPtr GetTransactionTrackerInvoker() const;

    //! Throws TLeaderFallbackException at followers.
    void RequireLeader() const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(THydraFacade)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
