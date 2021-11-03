#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra/mutation.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/core/rpc/public.h>

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
    void LoadSnapshot(
        NHydra::ISnapshotReaderPtr reader,
        bool dump,
        bool enableTotalWriteCountReport,
        const TSerializationDumperConfigPtr& dumpConfig);

    const TMasterAutomatonPtr& GetAutomaton() const;
    const NElection::IElectionManagerPtr& GetElectionManager() const;
    const NHydra::IHydraManagerPtr& GetHydraManager() const;
    const NRpc::TResponseKeeperPtr& GetResponseKeeper() const;

    IInvokerPtr GetAutomatonInvoker(EAutomatonThreadQueue queue) const;
    IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue) const;
    IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue) const;

    IInvokerPtr GetTransactionTrackerInvoker() const;

    void BlockAutomaton();
    void UnblockAutomaton();

    bool IsAutomatonLocked() const;

    void VerifyPersistentStateRead();

    //! Throws TLeaderFallbackException at followers.
    void RequireLeader() const;

    void Reconfigure(const TDynamicCellMasterConfigPtr& newConfig);

    const NObjectServer::TEpochContextPtr& GetEpochContext() const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(THydraFacade)

////////////////////////////////////////////////////////////////////////////////

class TAutomatonBlockGuard
{
public:
    TAutomatonBlockGuard(THydraFacadePtr hydraFacade);

    ~TAutomatonBlockGuard();

private:
    const THydraFacadePtr HydraFacade_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
