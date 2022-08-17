#include "config.h"

#include <yt/yt/server/node/cluster_node/config.h>

namespace NYT::NCellarNode {

////////////////////////////////////////////////////////////////////////////////

void TMasterConnectorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("heartbeat_period", &TThis::HeartbeatPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("heartbeat_period_splay", &TThis::HeartbeatPeriodSplay)
        .Default(TDuration::Seconds(1));
}

////////////////////////////////////////////////////////////////////////////////

void TMasterConnectorDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("heartbeat_period", &TThis::HeartbeatPeriod)
        .Default();
    registrar.Parameter("heartbeat_period_splay", &TThis::HeartbeatPeriodSplay)
        .Default();
    registrar.Parameter("heartbeat_timeout", &TThis::HeartbeatTimeout)
        .Default(TDuration::Seconds(60));
}

////////////////////////////////////////////////////////////////////////////////

void TCellarNodeDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cellar_manager", &TThis::CellarManager)
        .DefaultNew();
    registrar.Parameter("master_connector", &TThis::MasterConnector)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TCellarNodeConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cellar_manager", &TThis::CellarManager)
        .DefaultNew();

    registrar.Parameter("master_connector", &TThis::MasterConnector)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TCpuLimits::Register(TRegistrar registrar)
{
    registrar.Parameter("write_thread_pool_size", &TThis::WriteThreadPoolSize)
        .GreaterThan(0)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TMemoryLimitsEnumIndexedVector TMemoryLimits::AsEnumIndexedVector() const
{
    TMemoryLimitsEnumIndexedVector result;

    auto populate = [&result] (EMemoryCategory category, std::optional<int> value) {
        if (value) {
            auto limit = New<NClusterNode::TMemoryLimit>();
            limit->Type = NNodeTrackerClient::EMemoryLimitType::Static;
            limit->Value = value;
            result[category] = limit;
        }
    };

    populate(EMemoryCategory::TabletStatic, TabletStatic);
    populate(EMemoryCategory::TabletDynamic, TabletDynamic);
    populate(EMemoryCategory::BlockCache, BlockCache);
    populate(EMemoryCategory::VersionedChunkMeta, VersionedChunkMeta);
    populate(EMemoryCategory::LookupRowsCache, LookupRowCache);

    return result;
}

void TMemoryLimits::Register(TRegistrar registrar)
{
    registrar.Parameter("tablet_static", &TThis::TabletStatic)
        .Optional();
    registrar.Parameter("tablet_dynamic", &TThis::TabletDynamic)
        .Optional();
    registrar.Parameter("block_cache", &TThis::BlockCache)
        .Optional();
    registrar.Parameter("versioned_chunk_meta", &TThis::VersionedChunkMeta)
        .Optional();
    registrar.Parameter("lookup_row_cache", &TThis::LookupRowCache)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TBundleDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cpu_limits", &TThis::CpuLimits)
        .DefaultNew();

    registrar.Parameter("memory_limits", &TThis::MemoryLimits)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarNode
