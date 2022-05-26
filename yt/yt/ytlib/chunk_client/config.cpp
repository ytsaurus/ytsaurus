#include "config.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

void TRemoteReaderOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("allow_fetching_seeds_from_master", &TThis::AllowFetchingSeedsFromMaster)
        .Default(true);

    registrar.Parameter("enable_p2p", &TThis::EnableP2P)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TRemoteWriterOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("allow_allocating_new_target_nodes", &TThis::AllowAllocatingNewTargetNodes)
        .Default(true);
    registrar.Parameter("medium_name", &TThis::MediumName)
        .Default(DefaultStoreMediumName);
    registrar.Parameter("placement_id", &TThis::PlacementId)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TDispatcherDynamicConfig::TDispatcherDynamicConfig()
{
    RegisterParameter("chunk_reader_pool_size", ChunkReaderPoolSize)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

TDispatcherConfig::TDispatcherConfig()
{
    RegisterParameter("chunk_reader_pool_size", ChunkReaderPoolSize)
        .Default(DefaultChunkReaderPoolSize);
}

TDispatcherConfigPtr TDispatcherConfig::ApplyDynamic(const TDispatcherDynamicConfigPtr& dynamicConfig) const
{
    auto mergedConfig = New<TDispatcherConfig>();
    mergedConfig->ChunkReaderPoolSize = dynamicConfig->ChunkReaderPoolSize.value_or(ChunkReaderPoolSize);
    mergedConfig->Postprocess();
    return mergedConfig;
}

////////////////////////////////////////////////////////////////////////////////

void TMultiChunkWriterOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("replication_factor", &TThis::ReplicationFactor)
        .GreaterThanOrEqual(1)
        .Default(DefaultReplicationFactor);
    registrar.Parameter("account", &TThis::Account);
    registrar.Parameter("chunks_vital", &TThis::ChunksVital)
        .Default(true);
    registrar.Parameter("chunks_movable", &TThis::ChunksMovable)
        .Default(true);
    registrar.Parameter("validate_resource_usage_increase", &TThis::ValidateResourceUsageIncrease)
        .Default(true);
    registrar.Parameter("erasure_codec", &TThis::ErasureCodec)
        .Default(NErasure::ECodec::None);
    registrar.Parameter("enable_striped_erasure", &TThis::EnableStripedErasure)
        .Default(false);
    registrar.Parameter("table_index", &TThis::TableIndex)
        .Default(InvalidTableIndex);
    registrar.Parameter("table_schema", &TThis::TableSchema)
        .Default();
    registrar.Parameter("chunk_consistent_replica_placement_hash", &TThis::ConsistentChunkReplicaPlacementHash)
        .Default(NChunkClient::NullConsistentReplicaPlacementHash);
}

////////////////////////////////////////////////////////////////////////////////

void TMultiChunkReaderOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("keep_in_memory", &TThis::KeepInMemory)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TMetaAggregatingWriterOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_skynet_sharing", &TThis::EnableSkynetSharing)
        .Default(false);
    registrar.Parameter("max_heavy_columns", &TThis::MaxHeavyColumns)
        .Default(0);
    registrar.Parameter("allow_unknown_extensions", &TThis::AllowUnknownExtensions)
        .Default(false);
    registrar.Parameter("max_block_count", &TThis::MaxBlockCount)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TBlockCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("compressed_data", &TThis::CompressedData)
        .DefaultNew();
    registrar.Parameter("uncompressed_data", &TThis::UncompressedData)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TBlockCacheDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("compressed_data", &TThis::CompressedData)
        .DefaultNew();
    registrar.Parameter("uncompressed_data", &TThis::UncompressedData)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

TChunkScraperConfig::TChunkScraperConfig()
{
    RegisterParameter("max_chunks_per_request", MaxChunksPerRequest)
        .Default(10000)
        .GreaterThan(0)
        .LessThan(100000);
}

////////////////////////////////////////////////////////////////////////////////

TChunkTeleporterConfig::TChunkTeleporterConfig()
{
    RegisterParameter("max_teleport_chunks_per_request", MaxTeleportChunksPerRequest)
        .GreaterThan(0)
        .Default(5000);
}

////////////////////////////////////////////////////////////////////////////////

TMediumDirectorySynchronizerConfig::TMediumDirectorySynchronizerConfig()
{
    RegisterParameter("sync_period", SyncPeriod)
        .Default(TDuration::Seconds(60));
}

////////////////////////////////////////////////////////////////////////////////

void TChunkFragmentReaderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("peer_info_expiration_timeout", &TThis::PeerInfoExpirationTimeout)
        .Default(TDuration::Minutes(30));

    registrar.Parameter("seeds_expiration_timeout", &TThis::SeedsExpirationTimeout)
        .Default(TDuration::Seconds(3));

    registrar.Parameter("periodic_update_delay", &TThis::PeriodicUpdateDelay)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::Seconds(10));

    registrar.Parameter("net_queue_size_factor", &TThis::NetQueueSizeFactor)
        .Default(0.5);
    registrar.Parameter("disk_queue_size_factor", &TThis::DiskQueueSizeFactor)
        .Default(1.0);

    registrar.Parameter("probe_chunk_set_rpc_timeout", &TThis::ProbeChunkSetRpcTimeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("get_chunk_fragment_set_rpc_timeout", &TThis::GetChunkFragmentSetRpcTimeout)
        .Default(TDuration::Seconds(15));

    registrar.Parameter("fragment_read_hedging_delay", &TThis::FragmentReadHedgingDelay)
        .Default();

    registrar.Parameter("retry_count_limit", &TThis::RetryCountLimit)
        .GreaterThanOrEqual(1)
        .Default(10);
    registrar.Parameter("retry_backoff_time", &TThis::RetryBackoffTime)
        .Default(TDuration::MilliSeconds(10));
    registrar.Parameter("read_time_limit", &TThis::ReadTimeLimit)
        .Default(TDuration::Seconds(15));

    registrar.Parameter("chunk_info_cache_expiration_timeout", &TThis::ChunkInfoCacheExpirationTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("suspicious_node_grace_period", &TThis::SuspiciousNodeGracePeriod)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("use_direct_io", &TThis::UseDirectIO)
        .Default(false)
        .DontSerializeDefault();
}

////////////////////////////////////////////////////////////////////////////////

void TChunkReplicaCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("expiration_time", &TThis::ExpirationTime)
        .Default(TDuration::Minutes(15));
    registrar.Parameter("expiration_sweep_period", &TThis::ExpirationSweepPeriod)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("max_chunks_per_locate", &TThis::MaxChunksPerLocate)
        .Default(1'000);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
