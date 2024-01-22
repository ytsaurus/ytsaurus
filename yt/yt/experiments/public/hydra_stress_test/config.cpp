#include "config.h"

namespace NYT::NHydraStressTest {

//////////////////////////////////////////////////////////////////////////////////

void TConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("peer_count", &TThis::PeerCount);
    registrar.Parameter("voting_peer_count", &TThis::VotingPeerCount);

    registrar.Parameter("client_count", &TThis::ClientCount);
    registrar.Parameter("client_increment", &TThis::ClientIncrement)
        .Default(100);
    registrar.Parameter("client_interval", &TThis::ClientInterval)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("client_write_cas_delay", &TThis::ClientWriteCasDelay)
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("default_proxy_timeout", &TThis::DefaultProxyTimeout)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("max_random_partition_count", &TThis::MaxRandomPartitionIterations)
        .Default(5);
    registrar.Parameter("random_partition_delay", &TThis::RandomPartitionDelay)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("quorum_partition_delay", &TThis::QuorumPartitionDelay)
        .Default(TDuration::Seconds(600));

    registrar.Parameter("clear_state_period", &TThis::ClearStatePeriod)
        .Default(TDuration::Seconds(50));

    registrar.Parameter("build_snapshot_perion", &TThis::BuildSnapshotPeriod)
        .Default(TDuration::Seconds(100));

    registrar.Parameter("leader_switch_period", &TThis::LeaderSwitchPeriod)
        .Default(TDuration::Seconds(200));

    registrar.Parameter("unavailability_timeout", &TThis::UnavailabilityTimeout)
        .Default(TDuration::Seconds(20));
    registrar.Parameter("resurrection_timeout", &TThis::ResurrectionTimeout)
        .Default(TDuration::Seconds(200));

    registrar.Parameter("hydra_manager", &TThis::HydraManager)
        .DefaultNew();
    registrar.Parameter("election_manager", &TThis::ElectionManager)
        .DefaultNew();
    registrar.Parameter("changelogs", &TThis::Changelogs)
        .DefaultNew();
    registrar.Parameter("snapshots", &TThis::Snapshots)
        .DefaultNew();
    registrar.Parameter("logging", &TThis::Logging)
        .Optional();

    registrar.Postprocessor([] (TThis* config) {
        if (config->VotingPeerCount == 0) {
            config->VotingPeerCount = config->PeerCount;
        }
    });
}

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest
