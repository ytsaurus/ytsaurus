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

TChunkFragmentReaderConfig::TChunkFragmentReaderConfig()
{
    RegisterParameter("chunk_replica_locator_expiration_timeout", ChunkReplicaLocatorExpirationTimeout)
        .Default(TDuration::Minutes(30));
    RegisterParameter("peer_info_expiration_timeout", PeerInfoExpirationTimeout)
        .Default(TDuration::Minutes(30));

    RegisterParameter("seeds_expiration_timeout", SeedsExpirationTimeout)
        .Default(TDuration::Seconds(3));

    RegisterParameter("periodic_update_delay", PeriodicUpdateDelay)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::Seconds(10));

    RegisterParameter("net_queue_size_factor", NetQueueSizeFactor)
        .Default(0.5);
    RegisterParameter("disk_queue_size_factor", DiskQueueSizeFactor)
        .Default(1.0);

    RegisterParameter("probe_chunk_set_rpc_timeout", ProbeChunkSetRpcTimeout)
        .Default(TDuration::Seconds(5));
    RegisterParameter("get_chunk_fragment_set_rpc_timeout", GetChunkFragmentSetRpcTimeout)
        .Default(TDuration::Seconds(15));

    RegisterParameter("get_chunk_fragment_multiplexing_parallelism", GetChunkFragmentSetMultiplexingParallelism)
        .GreaterThan(0)
        .Default(1);

    RegisterParameter("max_retry_count", MaxRetryCount)
        .GreaterThanOrEqual(1)
        .Default(3);
    RegisterParameter("retry_backoff_time", RetryBackoffTime)
        .Default(TDuration::MilliSeconds(10));

    RegisterParameter("evict_after_successful_access_time", EvictAfterSuccessfulAccessTime)
        .Default(TDuration::Seconds(30));

    RegisterParameter("suspicious_node_grace_period", SuspiciousNodeGracePeriod)
        .Default(TDuration::Minutes(5));

    RegisterParameter("use_direct_io", UseDirectIO)
        .Default(false)
        .DontSerializeDefault();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
