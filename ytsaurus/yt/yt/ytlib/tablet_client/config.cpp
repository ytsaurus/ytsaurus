#include "config.h"

namespace NYT::NTabletClient {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

void TTabletCellOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("peer_count", &TThis::PeerCount)
        .Default(1)
        .InRange(1, MaxPeerCount);
    registrar.Parameter("independent_peers", &TThis::IndependentPeers)
        .Default(false);
    registrar.Parameter("clock_cluster_tag", &TThis::ClockClusterTag)
        .Default(InvalidCellTag);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicTabletCellOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("cpu_per_tablet_slot", &TThis::CpuPerTabletSlot)
        .Optional();
    registrar.Parameter("suppress_tablet_cell_decommission", &TThis::SuppressTabletCellDecommission)
        .Optional();
    registrar.Parameter("forced_rotation_memory_ratio", &TThis::ForcedRotationMemoryRatio)
        .InRange(0.0, 1.0)
        .Default(0.7);
    registrar.Parameter("dynamic_memory_pool_weight", &TThis::DynamicMemoryPoolWeight)
        .InRange(1, MaxDynamicMemoryPoolWeight)
        .Default(1);
    registrar.Parameter("enable_tablet_dynamic_memory_limit", &TThis::EnableTabletDynamicMemoryLimit)
        .Default(true);
    registrar.Parameter("solomon_tag", &TThis::SolomonTag)
        .Optional()
        .DontSerializeDefault();
    registrar.Parameter("max_backing_store_memory_ratio", &TThis::MaxBackingStoreMemoryRatio)
        .Default(0.15);
    registrar.Parameter("increase_upload_replication_factor", &TThis::IncreaseUploadReplicationFactor)
        .Default(false);
    registrar.Parameter("ban_message", &TThis::BanMessage)
        .Optional();

    registrar.Postprocessor([] (TThis* config) {
        if (config->MaxBackingStoreMemoryRatio &&
            *config->MaxBackingStoreMemoryRatio + config->ForcedRotationMemoryRatio >= 1.0)
        {
            THROW_ERROR_EXCEPTION("\"max_backing_store_memory_ratio\" + "
                "\"forced_rotation_memory_ratio\" should be less than 1");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
