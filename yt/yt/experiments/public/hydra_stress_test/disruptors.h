#pragma once

#include "public.h"

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/ytlib/hydra/hydra_service_proxy.h>

namespace NYT::NHydraStressTest {

//////////////////////////////////////////////////////////////////////////////////

class TLeaderSwitcher
    : public TRefCounted
{
public:
    TLeaderSwitcher(
        TConfigPtr config,
        TLivenessCheckerPtr livenessChecker,
        std::vector<TString> addresses,
        std::vector<NRpc::IChannelPtr> peerChannels,
        NRpc::IChannelPtr leaderChannel);

    void Run();

private:
    const TConfigPtr Config_;
    const TLivenessCheckerPtr LivenessChecker_;
    const std::vector<TString> Addresses_;
    const std::vector<NRpc::IChannelPtr> PeerChannels_;
    const NRpc::IChannelPtr LeaderChannel_;

    void DoRun();
};

DEFINE_REFCOUNTED_TYPE(TLeaderSwitcher)

//////////////////////////////////////////////////////////////////////////////////

class TSnapshotBuilder
    : public TRefCounted
{
public:
    TSnapshotBuilder(
        TConfigPtr config,
        const std::vector<TPeerPtr>& peers,
        TLivenessCheckerPtr livenessChecker,
        NRpc::IChannelPtr peerChannel);

    void Run();

private:
    const TConfigPtr Config_;
    const std::vector<TPeerPtr> Peers_;
    const TLivenessCheckerPtr LivenessChecker_;

    NHydra::THydraServiceProxy Proxy_;

    void DoRun();
};

DEFINE_REFCOUNTED_TYPE(TSnapshotBuilder)

//////////////////////////////////////////////////////////////////////////////////

class TPersistenceDestroyer
    : public TRefCounted
{
public:
    TPersistenceDestroyer(
        TConfigPtr config,
        const std::vector<TPeerPtr>& peers,
        TLivenessCheckerPtr livenessChecker,
        NRpc::IChannelPtr peerChannel);

    void Run();

private:
    const TConfigPtr Config_;
    const std::vector<TPeerPtr> Peers_;
    const TLivenessCheckerPtr LivenessChecker_;
    const NRpc::IChannelPtr PeerChannel_;

    bool ClearingPeer_ = false;

    void ClearPeer(int peerIndex);
    void DoRun();
};

DEFINE_REFCOUNTED_TYPE(TPersistenceDestroyer)

//////////////////////////////////////////////////////////////////////////////////

class TNetworkDisruptor
    : public TRefCounted
{
public:
    TNetworkDisruptor(TChannelManagerPtr channelManager,
        const std::vector<TPeerPtr>& peers,
        TConfigPtr config,
        TLivenessCheckerPtr livenessChecker);

    void Run();

private:
    const TChannelManagerPtr ChannelManager_;
    const TConfigPtr Config_;
    const TLivenessCheckerPtr LivenessChecker_;

    std::vector<TPeerPtr> Peers_;

    void SplitIntoGroups(int groupCount, bool keepQuorum);
    void SplitRandomly();

    void DoRun();
};

DEFINE_REFCOUNTED_TYPE(TNetworkDisruptor)

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest
