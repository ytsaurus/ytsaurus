#include "disruptors.h"
#include "helpers.h"
#include "peer.h"
#include "peer_proxy.h"
#include "channel.h"
#include "config.h"

#include <yt/yt/ytlib/hydra/helpers.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/utilex/random.h>

#include <yt/yt/core/misc/fs.h>

namespace NYT::NHydraStressTest {

using namespace NRpc;
using namespace NConcurrency;
using namespace NHydra;

//////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HydraStressTestLogger;

//////////////////////////////////////////////////////////////////////////////////

TLeaderSwitcher::TLeaderSwitcher(
    TConfigPtr config,
    TLivenessCheckerPtr livenessChecker,
    std::vector<TString> addresses,
    std::vector<IChannelPtr> peerChannels,
    IChannelPtr leaderChannel)
    : Config_(config)
    , LivenessChecker_(livenessChecker)
    , Addresses_(std::move(addresses))
    , PeerChannels_(std::move(peerChannels))
    , LeaderChannel_(std::move(leaderChannel))
{ }

void TLeaderSwitcher::Run()
{
    YT_UNUSED_FUTURE(BIND(&TLeaderSwitcher::DoRun, MakeStrong(this))
        .AsyncVia(GetCurrentInvoker())
        .Run());
}

void TLeaderSwitcher::DoRun()
{
    while (true) {
        TDelayedExecutor::WaitForDuration(Config_->LeaderSwitchPeriod + RandomDuration(TDuration::Seconds(20)));

        LivenessChecker_->IncrementErrorCount(+1);

        auto newLeaderId = rand() % PeerChannels_.size();
        SwitchLeader(
            PeerChannels_,
            LeaderChannel_,
            PeerChannels_[newLeaderId],
            Addresses_,
            Addresses_[newLeaderId],
            Config_->DefaultProxyTimeout,
            "HydraStressTest");

        LivenessChecker_->IncrementErrorCount(-1);
    }
}

//////////////////////////////////////////////////////////////////////////////////

TSnapshotBuilder::TSnapshotBuilder(
    TConfigPtr config,
    const std::vector<TPeerPtr>& peers,
    TLivenessCheckerPtr livenessChecker,
    IChannelPtr peerChannel)
    : Config_(config)
    , Peers_(peers)
    , LivenessChecker_(livenessChecker)
    , Proxy_(peerChannel)
{ }

void TSnapshotBuilder::Run()
{
    YT_UNUSED_FUTURE(BIND(&TSnapshotBuilder::DoRun, MakeStrong(this))
        .AsyncVia(GetCurrentInvoker())
        .Run());
}

void TSnapshotBuilder::DoRun()
{
    while (true) {
        TDelayedExecutor::WaitForDuration(Config_->BuildSnapshotPeriod + RandomDuration(TDuration::Seconds(20)));

        bool readOnly = false;
        while (true) {
            auto req = Proxy_.ForceBuildSnapshot();
            req->SetTimeout(TDuration::Hours(1));
            req->set_set_read_only(readOnly);
            req->set_wait_for_snapshot_completion(true);

            YT_LOG_INFO("Building snapshot (ReadOnly: %v)", readOnly);

            auto rsp = WaitFor(req->Invoke());
            if (rsp.IsOK()) {
                YT_LOG_INFO("Snapshot built (SnapshotId: %v)", rsp.Value()->snapshot_id());
                break;
            }
            YT_LOG_INFO(rsp, "Failed building snapshot");
            TDelayedExecutor::WaitForDuration(TDuration::Seconds(3));
        }

        // TODO
        if (readOnly) {
            for (const auto& peer : Peers_) {
                WaitFor(peer->Finalize())
                    .ThrowOnError();
                peer->Initialize();
            }
            LivenessChecker_->IncrementErrorCount(-1);
        }
    }
}

//////////////////////////////////////////////////////////////////////////////////

TPersistenceDestroyer::TPersistenceDestroyer(
    TConfigPtr config,
    const std::vector<TPeerPtr>& peers,
    TLivenessCheckerPtr livenessChecker,
    IChannelPtr peerChannel)
    : Config_(config)
    , Peers_(peers)
    , LivenessChecker_(livenessChecker)
    , PeerChannel_(peerChannel)
{ }

void TPersistenceDestroyer::Run()
{
    YT_UNUSED_FUTURE(BIND(&TPersistenceDestroyer::DoRun, MakeStrong(this))
        .AsyncVia(GetCurrentInvoker())
        .Run());
}

void TPersistenceDestroyer::ClearPeer(int peerIndex)
{
    ClearingPeer_ = true;
    auto& peer = Peers_[peerIndex];
    auto peerId = peer->GetPeerId();
    YT_LOG_INFO("Killing peer (PeerId: %v)", peerId);

    YT_VERIFY(WaitFor(peer->Finalize()).IsOK());

    if (peer->IsVoting()) {
        while (true) {
            YT_LOG_DEBUG("Waiting for quorum to become alive");

            TPeerServiceProxy proxy(PeerChannel_);
            proxy.SetDefaultTimeout(Config_->DefaultProxyTimeout);

            auto req = proxy.Read();

            auto result = WaitFor(req->Invoke());
            if (result.IsOK()) {
                break;
            }
            TDelayedExecutor::WaitForDuration(TDuration::Seconds(3));
        }
    }
    YT_LOG_DEBUG("Quorum is alive");

    if (rand() % 2) {
        YT_LOG_INFO("Removing snapshots and changelogs (PeerId: %v)", peerId);

        NFS::RemoveRecursive(Config_->Snapshots->Path + "/" + ToString(peerId));
        NFS::RemoveRecursive(Config_->Changelogs->Path + "/" + ToString(peerId));
    }

    YT_LOG_INFO("Started resurrecting peer (PeerId: %v)", peerId);
    peer->Initialize();
    YT_LOG_INFO("Peer resurrected (PeerId: %v)", peerId);
    ClearingPeer_ = false;
}

void TPersistenceDestroyer::DoRun()
{
    while (true) {
        TDelayedExecutor::WaitForDuration(Config_->ClearStatePeriod);
        if (ClearingPeer_) {
            YT_LOG_INFO("Waiting for peer to be resurrected");
            continue;
        }
        auto peerIdx = rand() % Config_->PeerCount;
        if (Peers_[peerIdx]->IsVoting()) {
            LivenessChecker_->IncrementErrorCount(+1);
            ClearPeer(peerIdx);
            LivenessChecker_->IncrementErrorCount(-1);
        } else {
            ClearPeer(peerIdx);
        }
    }
}

//////////////////////////////////////////////////////////////////////////////////

TNetworkDisruptor::TNetworkDisruptor(
    TChannelManagerPtr channelManager,
    const std::vector<TPeerPtr>& peers,
    TConfigPtr config,
    TLivenessCheckerPtr livenessChecker)
    : ChannelManager_(channelManager)
    , Config_(config)
    , LivenessChecker_(livenessChecker)
    , Peers_(peers)
{ }

void TNetworkDisruptor::Run()
{
    YT_UNUSED_FUTURE(BIND(&TNetworkDisruptor::DoRun, MakeStrong(this))
        .AsyncVia(GetCurrentInvoker())
        .Run());
}

void TNetworkDisruptor::SplitIntoGroups(int groupCount, bool keepQuorum)
{
    int peerCount = Config_->PeerCount;
    std::vector<int> groupIndex(peerCount);

    for (int i = 0; i < peerCount; ++i) {
        groupIndex[i] = rand() % groupCount;
    }

    if (keepQuorum && groupCount > 1) {
        std::vector<int> votingPeerIndices;
        int deadVotingPeers = 0;
        for (int i = 0; i < peerCount; ++i) {
            const auto& peer = Peers_[i];
            if (peer->IsVoting()) {
                groupIndex[i] = 0;
                if (peer->IsRecovery() || peer->IsActive()) {
                    votingPeerIndices.push_back(i);
                } else {
                    ++deadVotingPeers;
                    YT_LOG_WARNING("Peer is not alive (PeerId: %v, State: %v)",
                        peer->GetPeerId(),
                        peer->GetAutomatonState());
                }
            }
        }

        for (int i = 0; i < Config_->VotingPeerCount / 2 - deadVotingPeers; ++i) {
            auto index = votingPeerIndices[rand() % std::ssize(votingPeerIndices)];
            groupIndex[index] = rand() % (groupCount - 1) + 1;
        }
    }

    for (int i = 0; i < peerCount; ++i) {
        const auto& peer = Peers_[i];
        YT_LOG_INFO("Split into groups (PeerId: %v, GroupId: %v)",
            peer->GetPeerId(),
            groupIndex[i]);
    }

    for (int from = 0; from < peerCount; ++from) {
        for (int to = from + 1; to < peerCount; ++to) {
            ChannelManager_->SetBroken(from, to, groupIndex[from] != groupIndex[to]);
        }
    }
}

void TNetworkDisruptor::SplitRandomly()
{
    int peerCount = Config_->PeerCount;
    for (int from = 0; from < peerCount; ++from) {
        for (int to = from + 1; to < peerCount; ++to) {
            bool broken = rand() % 2;
            ChannelManager_->SetBroken(from, to, broken);
        }
    }
}

void TNetworkDisruptor::DoRun()
{
    YT_LOG_INFO("Starting breaking connections");

    while (true) {
        LivenessChecker_->IncrementErrorCount(+1);
        int randomPartitionIterations = rand() % Config_->MaxRandomPartitionIterations;
        for (int i = 0; i < randomPartitionIterations; ++i) {
            YT_LOG_INFO("Breaking connections randomly");
            SplitRandomly();
            TDelayedExecutor::WaitForDuration(Config_->RandomPartitionDelay);
        }

        int partsCount = rand() % (Config_->PeerCount / 2 + 1) + 1;
        YT_LOG_INFO("Splitting into groups (PartsCount: %v)", partsCount);
        SplitIntoGroups(partsCount, true);
        LivenessChecker_->IncrementErrorCount(-1);
        YT_LOG_INFO("Quorum must be alive");
        TDelayedExecutor::WaitForDuration(Config_->QuorumPartitionDelay);
    }
}

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest
