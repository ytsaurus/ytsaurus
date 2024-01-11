#pragma once

#include "public.h"
#include "automaton.h"
#include "config.h"

#include <yt/yt/core/misc/public.h>

#include <yt/yt/server/lib/hydra/public.h>
#include <yt/yt/server/lib/hydra/hydra_service.h>

#include <yt/yt/client/hydra/public.h>

namespace NYT::NHydraStressTest {

//////////////////////////////////////////////////////////////////////////////////

class TPeer
    : public TRefCounted
{
public:
    TPeer(
        TConfigPtr config,
        NElection::TCellConfigPtr cellConfig,
        NRpc::IChannelFactoryPtr channelFactory,
        int peerId,
        IInvokerPtr snapshotIOInvoker,
        TLinearizabilityCheckerPtr linearizabilityChecker,
        bool voting = true);

    void Initialize();
    TFuture<void> Finalize();

    bool IsActive() const;
    bool IsRecovery() const;
    bool IsVoting() const;

    NHydra::EPeerState GetAutomatonState() const;
    NRpc::IChannelPtr GetChannel() const;
    int GetPeerId() const;
    IInvokerPtr GetAutomatonInvoker() const;

private:
    friend class TPeerService;

    const TConfigPtr Config_;
    const NElection::TCellConfigPtr CellConfig_;
    const IInvokerPtr SnapshotIOInvoker_;
    const TLinearizabilityCheckerPtr LinearizabilityChecker_;
    const int PeerId_;
    const bool Voting_;
    const NElection::TCellManagerPtr CellManager_;
    const NConcurrency::TActionQueuePtr AutomatonQueue_;
    const NConcurrency::TActionQueuePtr ControlQueue_;
    const NElection::TElectionManagerThunkPtr ElectionManagerThunk_;
    const NRpc::IServerPtr RpcServer_;
    const NRpc::IChannelPtr Channel_;

    TAutomatonPartPtr AutomatonPart_;
    NHydra::IHydraManagerPtr HydraManager_;
    NRpc::IServicePtr PeerService_;
};

DEFINE_REFCOUNTED_TYPE(TPeer)

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest
