#include "config.h"

namespace NYT::NIncumbentServer {

using namespace NIncumbentClient;

////////////////////////////////////////////////////////////////////////////////

void TIncumbentSchedulingConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("use_followers", &TIncumbentSchedulingConfig::UseFollowers)
        .Default(false);

    registrar.Parameter("weight", &TThis::Weight)
        .Default(1);
}

////////////////////////////////////////////////////////////////////////////////

void TIncumbentSchedulerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("incumbents", &TThis::Incumbents)
        .DefaultCtor([] {
            static const THashSet<EIncumbentType> UseFollowersIncumbents = {
                EIncumbentType::ChunkReplicator,
                EIncumbentType::CellJanitor
            };
            TEnumIndexedArray<EIncumbentType, TIncumbentSchedulingConfigPtr> incumbents;

            for (auto incumbentType : TEnumTraits<EIncumbentType>::GetDomainValues()) {
                incumbents[incumbentType] = New<TIncumbentSchedulingConfig>();
                incumbents[incumbentType]->Weight = 1;
                incumbents[incumbentType]->UseFollowers = UseFollowersIncumbents.contains(incumbentType);
            }

            return incumbents;
        })
        .ResetOnLoad();

    registrar.Parameter("min_alive_followers", &TThis::MinAliveFollowers)
        .Default(0);

    registrar.Postprocessor([] (TThis* config) {
        for (auto incumbentType : TEnumTraits<EIncumbentType>::GetDomainValues()) {
            auto& incumbent = config->Incumbents[incumbentType];
            if (!incumbent) {
                incumbent = New<TIncumbentSchedulingConfig>();
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TIncumbentManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("scheduler", &TThis::Scheduler)
        .DefaultNew();

    registrar.Parameter("assign_period", &TThis::AssignPeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("peer_lease_duration", &TThis::PeerLeaseDuration)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("peer_grace_period", &TThis::PeerGracePeriod)
        .Default(TDuration::Seconds(45));

    registrar.Parameter("banned_peers", &TThis::BannedPeers)
        .Default();

    registrar.Parameter("heartbeat_timeout", &TThis::HeartbeatTimeout)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIncumbentServer
