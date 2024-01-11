#include "automaton.h"
#include "channel.h"
#include "checkers.h"
#include "client.h"
#include "config.h"
#include "disruptors.h"
#include "helpers.h"
#include "peer.h"
#include "public.h"

#include <library/cpp/getopt/last_getopt.h>

#include <yt/yt/ytlib/election/cell_manager.h>
#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/ytlib/hydra/config.h>
#include <yt/yt/ytlib/hydra/peer_channel.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/rpc/static_channel_factory.h>

#include <yt/yt/server/lib/election/distributed_election_manager.h>

#include <yt/yt/server/lib/hydra/config.h>

#include <random>


namespace NYT::NHydraStressTest {

using namespace NBus;
using namespace NRpc;
using namespace NHydra;
using namespace NYTree;
using namespace NElection;
using namespace NConcurrency;

//////////////////////////////////////////////////////////////////////////////////

void RunHydraTest(const TString& configFile)
{
    const auto& Logger = HydraStressTestLogger;
    TConfigPtr config;
    try {
        TIFStream configStream(configFile);
        auto configNode = ConvertToNode(&configStream);
        config = New<TConfig>();
        config->Load(configNode);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error reading configuration") << ex;
    }

    if (config->Logging) {
        NLogging::TLogManager::Get()->Configure(config->Logging);
    }

    std::vector<int> peerIndices(config->PeerCount);
    std::iota(peerIndices.begin(), peerIndices.end(), 0);
    std::shuffle(peerIndices.begin(), peerIndices.end(), std::mt19937{123});

    auto cellConfig = New<TCellConfig>();
    for (int i = 0; i < config->PeerCount; ++i) {
        bool voting = peerIndices[i] < config->VotingPeerCount;
        YT_LOG_INFO("Peer created (PeerId: %v, Voting: %v)", i, voting);
        auto peerConfig = New<TCellPeerConfig>();
        peerConfig->Address = GetPeerAddress(i);
        peerConfig->Voting = voting;
        cellConfig->Peers.push_back(peerConfig);
    }

    auto channelManager = New<TChannelManager>(config->PeerCount);
    auto snapshotIOActionQueue = New<TActionQueue>("SnapshotIO");
    auto snapshotIOInvoker = snapshotIOActionQueue->GetInvoker();
    auto linearizabilityChecker = New<TLinearizabilityChecker>(config->PeerCount);
    std::vector<TPeerPtr> peers;
    for (int i = 0; i < config->PeerCount; ++i) {
        auto channelFactory = channelManager->GetChannelFactory(i);
        auto peer = New<TPeer>(
            config,
            cellConfig,
            channelFactory,
            i,
            snapshotIOInvoker,
            linearizabilityChecker,
            cellConfig->Peers[i]->Voting);
        peers.push_back(peer);
        channelManager->SetUnderlying(peer->GetChannel(), i);
    }

    std::vector<IChannelPtr> peerChannels;

    auto peerChannelFactory = New<TStaticChannelFactory>();
    auto peerConnectionConfig = New<TPeerConnectionConfig>();
    peerConnectionConfig->Addresses.emplace();
    for (int i = 0; i < config->PeerCount; ++i) {
        auto address = GetPeerAddress(i);
        auto channel = peers[i]->GetChannel();
        peerChannelFactory->Add(address, channel);
        peerChannels.push_back(channel);
        peerConnectionConfig->Addresses->push_back(address);
    }

    auto peerChannel = CreatePeerChannel(peerConnectionConfig, peerChannelFactory, EPeerKind::LeaderOrFollower);

    auto clientActionQueue = New<TActionQueue>("Client");
    auto clientInvoker = clientActionQueue->GetInvoker();

    std::vector<TClientPtr> clients;
    auto livenessChecker = New<TLivenessChecker>(config);
    for (int i = 0; i < config->ClientCount; ++i) {
        auto client = New<TClient>(
            config,
            peerChannel,
            clientInvoker,
            livenessChecker,
            i);
        clients.push_back(client);
        client->Run();
    }

    for (const auto& peer: peers) {
        peer->Initialize();
    }

    auto disruptorActionQueue = New<TActionQueue>("Disruptor");
    disruptorActionQueue->GetInvoker()->Invoke(
        BIND([&] {
            auto networkDisruptor = New<TNetworkDisruptor>(channelManager, peers, config, livenessChecker);
            networkDisruptor->Run();

            auto leaderChannel = CreatePeerChannel(peerConnectionConfig, peerChannelFactory, EPeerKind::Leader);
            auto snapshotBuilder = New<TSnapshotBuilder>(
                config,
                peers,
                livenessChecker,
                leaderChannel);
            snapshotBuilder->Run();

            auto leaderSwitcher = New<TLeaderSwitcher>(
                config,
                livenessChecker,
                *peerConnectionConfig->Addresses,
                peerChannels,
                leaderChannel);
            leaderSwitcher->Run();

            // auto persistenceDestroyer = New<TPersistenceDestroyer>(config, peers, livenessChecker, peerChannel);
            // persistenceDestroyer->Run();
        }));

    Sleep(TDuration::Max());
}

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest

int main(int argc, const char** argv)
{
    try {
        NLastGetopt::TOpts opts;

        TString Config;
        opts.AddLongOption("config", "Config").StoreResult(&Config);
        NLastGetopt::TOptsParseResult results(&opts, argc, argv);

        NYT::NHydraStressTest::RunHydraTest(Config);
        return 0;
    } catch (const std::exception& ex) {
        Cerr << ToString(NYT::TError(ex)) << Endl;
        return 1;
    }
}
