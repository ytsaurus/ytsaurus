#include "config.h"

namespace NYT::NChunkClient {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TRemoteReaderOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("allow_fetching_seeds_from_master", &TThis::AllowFetchingSeedsFromMaster)
        .Default(true);

    registrar.Parameter("enable_p2p", &TThis::EnableP2P)
        .Default(false);

    registrar.Parameter("use_proxying_data_node_service", &TThis::UseProxyingDataNodeService)
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

TDispatcherConfigPtr TDispatcherConfig::ApplyDynamic(
    const TDispatcherDynamicConfigPtr& dynamicConfig) const
{
    auto mergedConfig = CloneYsonStruct(MakeStrong(this));
    UpdateYsonStructField(mergedConfig->ChunkReaderPoolSize, dynamicConfig->ChunkReaderPoolSize);
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
    registrar.Parameter("chunk_fragments_data", &TThis::ChunkFragmentsData)
        .DefaultNew();
    registrar.Parameter("min_hash_digest", &TThis::MinHashDigest)
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
    registrar.Parameter("chunk_fragments_data", &TThis::ChunkFragmentsData)
        .DefaultNew();
    registrar.Parameter("min_hash_digest", &TThis::MinHashDigest)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TChunkScraperConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_chunks_per_request", &TThis::MaxChunksPerRequest)
        .Default(10000)
        .GreaterThan(0)
        .LessThan(100000);
    registrar.Parameter("prioritize_unavailable_chunks", &TThis::PrioritizeUnavailableChunks)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TChunkTeleporterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_teleport_chunks_per_request", &TThis::MaxTeleportChunksPerRequest)
        .GreaterThan(0)
        .Default(5000);
}

////////////////////////////////////////////////////////////////////////////////

void TS3MediumConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("bucket", &TThis::Bucket)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TMediumDirectorySynchronizerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("sync_period", &TThis::SyncPeriod)
        .Default(TDuration::Seconds(60));
}

////////////////////////////////////////////////////////////////////////////////

void TChunkReplicaCacheDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("expiration_time", &TThis::ExpirationTime)
        .Default();
    registrar.Parameter("expiration_sweep_period", &TThis::ExpirationSweepPeriod)
        .Default();
    registrar.Parameter("max_chunks_per_master_locate", &TThis::MaxChunksPerMasterLocate)
        .GreaterThan(0)
        .Default();
    registrar.Parameter("enable_sequoia_replicas_locate", &TThis::EnableSequoiaReplicasLocate)
        .Default();
    registrar.Parameter("enable_sequoia_replicas_refresh", &TThis::EnableSequoiaReplicasRefresh)
        .Default();
    registrar.Parameter("sequoia_replicas_refresh_period", &TThis::SequoiaReplicasRefreshPeriod)
        .Default();
    registrar.Parameter("max_chunks_per_sequoia_refresh_round", &TThis::MaxChunksPerSequoiaRefreshRound)
        .GreaterThan(0)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TChunkReplicaCacheConfigPtr TChunkReplicaCacheConfig::ApplyDynamic(const TChunkReplicaCacheDynamicConfigPtr& dynamicConfig) const
{
    auto mergedConfig = CloneYsonStruct(MakeStrong(this));
    UpdateYsonStructField(mergedConfig->ExpirationTime, dynamicConfig->ExpirationTime);
    UpdateYsonStructField(mergedConfig->ExpirationSweepPeriod, dynamicConfig->ExpirationSweepPeriod);
    UpdateYsonStructField(mergedConfig->MaxChunksPerMasterLocate, dynamicConfig->MaxChunksPerMasterLocate);
    UpdateYsonStructField(mergedConfig->EnableSequoiaReplicasLocate, dynamicConfig->EnableSequoiaReplicasLocate);
    UpdateYsonStructField(mergedConfig->EnableSequoiaReplicasRefresh, dynamicConfig->EnableSequoiaReplicasRefresh);
    UpdateYsonStructField(mergedConfig->SequoiaReplicasRefreshPeriod, dynamicConfig->SequoiaReplicasRefreshPeriod);
    UpdateYsonStructField(mergedConfig->MaxChunksPerSequoiaRefreshRound, dynamicConfig->MaxChunksPerSequoiaRefreshRound);
    mergedConfig->Postprocess();
    return mergedConfig;
}

void TChunkReplicaCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("expiration_time", &TThis::ExpirationTime)
        .Default(TDuration::Minutes(15));
    registrar.Parameter("expiration_sweep_period", &TThis::ExpirationSweepPeriod)
        .Default(TDuration::Minutes(15));
    registrar.Parameter("max_chunks_per_master_locate", &TThis::MaxChunksPerMasterLocate)
        .GreaterThan(0)
        .Default(1'000);
    registrar.Parameter("enable_sequoia_replicas_locate", &TThis::EnableSequoiaReplicasLocate)
        .Default(false);
    registrar.Parameter("enable_sequoia_replicas_refresh", &TThis::EnableSequoiaReplicasRefresh)
        .Default(false);
    registrar.Parameter("sequoia_replicas_refresh_period", &TThis::SequoiaReplicasRefreshPeriod)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("max_chunks_per_sequoia_refresh_round", &TThis::MaxChunksPerSequoiaRefreshRound)
        .GreaterThan(0)
        .Default(10'000);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
