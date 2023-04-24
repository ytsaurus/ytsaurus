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

void TDispatcherDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("chunk_reader_pool_size", &TThis::ChunkReaderPoolSize)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TDispatcherConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("chunk_reader_pool_size", &TThis::ChunkReaderPoolSize)
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
    registrar.Parameter("hash_table_chunk_index", &TThis::HashTableChunkIndex)
        .DefaultNew();
    registrar.Parameter("xor_filter", &TThis::XorFilter)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TBlockCacheDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("compressed_data", &TThis::CompressedData)
        .DefaultNew();
    registrar.Parameter("uncompressed_data", &TThis::UncompressedData)
        .DefaultNew();
    registrar.Parameter("hash_table_chunk_index", &TThis::HashTableChunkIndex)
        .DefaultNew();
    registrar.Parameter("xor_filter", &TThis::XorFilter)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TChunkScraperConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_chunks_per_request", &TThis::MaxChunksPerRequest)
        .Default(10000)
        .GreaterThan(0)
        .LessThan(100000);
}

////////////////////////////////////////////////////////////////////////////////

void TChunkTeleporterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_teleport_chunks_per_request", &TThis::MaxTeleportChunksPerRequest)
        .GreaterThan(0)
        .Default(5000);
}

////////////////////////////////////////////////////////////////////////////////

void TMediumDirectorySynchronizerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("sync_period", &TThis::SyncPeriod)
        .Default(TDuration::Seconds(60));
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
