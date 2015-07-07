#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <core/ytree/public.h>

#include <ytlib/election/public.h>

#include <ytlib/object_client/public.h>

#include <server/hydra/mutation.h>
#include <core/rpc/client.h>

namespace NYT {
namespace NCellMaster {

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

    void Start();
    void DumpSnapshot(NHydra::ISnapshotReaderPtr reader);

    TMasterAutomatonPtr GetAutomaton() const;
    NHydra::IHydraManagerPtr GetHydraManager() const;
    NRpc::TResponseKeeperPtr GetResponseKeeper() const;

    IInvokerPtr GetAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const;
    IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const;
    IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const;

    bool IsPrimaryMaster() const;
    bool IsSecondaryMaster() const;

    const NElection::TCellId& GetCellId() const;
    NObjectClient::TCellTag GetCellTag() const;

    const NElection::TCellId& GetPrimaryCellId() const;
    NObjectClient::TCellTag GetPrimaryCellTag() const;

    void PostToSecondaryMasters(NRpc::IClientRequestPtr request);
    void PostToSecondaryMasters(const NObjectClient::TObjectId& objectId, NRpc::IServiceContextPtr context);
    void PostToSecondaryMasters(TSharedRefArray requestMessage);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(THydraFacade)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
