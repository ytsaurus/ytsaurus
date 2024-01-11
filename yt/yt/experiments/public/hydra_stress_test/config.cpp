#include "config.h"

namespace NYT::NHydraStressTest {

//////////////////////////////////////////////////////////////////////////////////

TConfig::TConfig()
{
    RegisterParameter("peer_count", PeerCount);
    RegisterParameter("voting_peer_count", VotingPeerCount)
        .Default(PeerCount);

    RegisterParameter("client_count", ClientCount);
    RegisterParameter("client_increment", ClientIncrement)
        .Default(100);
    RegisterParameter("client_interval", ClientInterval)
        .Default(TDuration::Seconds(1));
    RegisterParameter("client_write_cas_delay", ClientWriteCasDelay)
        .Default(TDuration::MilliSeconds(100));

    RegisterParameter("default_proxy_timeout", DefaultProxyTimeout)
        .Default(TDuration::Seconds(10));

    RegisterParameter("max_random_partition_count", MaxRandomPartitionIterations)
        .Default(5);
    RegisterParameter("random_partition_delay", RandomPartitionDelay)
        .Default(TDuration::Seconds(10));
    RegisterParameter("quorum_partition_delay", QuorumPartitionDelay)
        .Default(TDuration::Seconds(600));

    RegisterParameter("clear_state_period", ClearStatePeriod)
        .Default(TDuration::Seconds(50));

    RegisterParameter("build_snapshot_perion", BuildSnapshotPeriod)
        .Default(TDuration::Seconds(100));

    RegisterParameter("leader_switch_period", LeaderSwitchPeriod)
        .Default(TDuration::Seconds(200));

    RegisterParameter("unavailability_timeout", UnavailabilityTimeout)
        .Default(TDuration::Seconds(20));
    RegisterParameter("resurrection_timeout", ResurrectionTimeout)
        .Default(TDuration::Seconds(200));

    RegisterParameter("hydra_manager", HydraManager)
        .DefaultNew();
    RegisterParameter("election_manager", ElectionManager)
        .DefaultNew();
    RegisterParameter("changelogs", Changelogs)
        .DefaultNew();
    RegisterParameter("snapshots", Snapshots)
        .DefaultNew();
    RegisterParameter("logging", Logging)
        .Default(nullptr);
}

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest
