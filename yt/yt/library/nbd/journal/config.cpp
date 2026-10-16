#include "config.h"

#include <yt/yt/client/api/config.h>

#include <yt/yt/client/chunk_client/config.h>

#include <yt/yt/client/journal_client/helpers.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <util/generic/bitops.h>

#include <limits>

namespace NYT::NNbd::NJournal {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TJournalBlockDeviceOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("device_size", &TThis::DeviceSize)
        .GreaterThan(0);
    registrar.Parameter("block_size", &TThis::BlockSize)
        .Default(MaxNbdBlockSize)
        .InRange(MinNbdBlockSize, MaxNbdBlockSize);
    registrar.Parameter("account", &TThis::Account);
    registrar.Parameter("medium_name", &TThis::MediumName);

    registrar.Postprocessor([] (TThis* config) {
        if (!IsPowerOf2(config->BlockSize)) {
            THROW_ERROR_EXCEPTION("\"block_size\" must be a power of two")
                .With("block_size", config->BlockSize);
        }
        if (config->DeviceSize % config->BlockSize != 0) {
            THROW_ERROR_EXCEPTION("\"device_size\" must be a multiple of \"block_size\"")
                .With("device_size", config->DeviceSize)
                .With("block_size", config->BlockSize);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TJournalBlockStoreConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("replication_factor", &TThis::ReplicationFactor)
        .Default(3)
        .GreaterThan(0);
    registrar.Parameter("read_quorum", &TThis::ReadQuorum)
        .Default(2)
        .GreaterThan(0);
    registrar.Parameter("write_quorum", &TThis::WriteQuorum)
        .Default(2)
        .GreaterThan(0);
    registrar.Parameter("write_parallelism", &TThis::WriteParallelism)
        .Default(2)
        .GreaterThan(0);
    registrar.Parameter("chunk_maintenance_period", &TThis::ChunkMaintenancePeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("max_chunk_data_size", &TThis::MaxChunkDataSize)
        .Default(10_GB)
        .GreaterThan(0);
    registrar.Parameter("dead_chunk_retention_delay", &TThis::DeadChunkRetentionDelay)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("write_backoff", &TThis::WriteBackoff)
        .Default({
            .InvocationCount = 10,
            .MinBackoff = TDuration::MilliSeconds(100),
            .MaxBackoff = TDuration::Seconds(3),
        });
    registrar.Parameter("chunk_creation_backoff", &TThis::ChunkCreationBackoff)
        .Default({
            .InvocationCount = 10,
            .MinBackoff = TDuration::Seconds(1),
            .MaxBackoff = TDuration::Seconds(30),
        });
    registrar.Parameter("seal_backoff", &TThis::SealBackoff)
        .Default({
            .InvocationCount = std::numeric_limits<int>::max(),
            .MinBackoff = TDuration::MilliSeconds(500),
            .MaxBackoff = TDuration::Seconds(10),
        });
    registrar.Parameter("seal_rpc_timeout", &TThis::SealRpcTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("seal_quorum_session_delay", &TThis::SealQuorumSessionDelay)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("snapshot_seal_timeout", &TThis::SnapshotSealTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("snapshot_flush_timeout", &TThis::SnapshotFlushTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("chunk_writer", &TThis::ChunkWriter)
        .DefaultNew();
    registrar.Parameter("chunk_reader", &TThis::ChunkReader)
        .DefaultNew();
    registrar.Parameter("read_hedging_manager", &TThis::ReadHedgingManager)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        NJournalClient::ValidateReplicatedJournalAttributes(
            config->ReplicationFactor,
            config->ReadQuorum,
            config->WriteQuorum);
    });
}

////////////////////////////////////////////////////////////////////////////////

void TJournalBlockFlusherConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("flush_period", &TThis::FlushPeriod)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("dirty_block_pool_capacity", &TThis::DirtyBlockPoolCapacity)
        .Default(128_MB)
        .GreaterThan(0);
    registrar.Parameter("dirty_fraction_threshold", &TThis::DirtyFractionThreshold)
        .Default(0.5)
        .InRange(0.0, 1.0);
}

////////////////////////////////////////////////////////////////////////////////

void TJournalBlockCompactorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("garbage_ratio_threshold", &TThis::GarbageRatioThreshold)
        .Default(0.5)
        .InRange(0.1, 0.9);
    registrar.Parameter("scan_period", &TThis::ScanPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("max_concurrent_compactions", &TThis::MaxConcurrentCompactions)
        .Default(1)
        .GreaterThan(0);
    registrar.Parameter("backoff", &TThis::Backoff)
        .Default({
            .InvocationCount = std::numeric_limits<int>::max(),
            .MinBackoff = TDuration::Seconds(15),
            .MaxBackoff = TDuration::Minutes(3),
        });
    registrar.Parameter("max_blocks_per_batch", &TThis::MaxBlocksPerBatch)
        .Default(10'000)
        .GreaterThan(0);
    registrar.Parameter("throughput_throttler", &TThis::ThroughputThrottler)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TJournalBlockDeviceDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("thread_pool_size", &TThis::ThreadPoolSize)
        .Default()
        .InRange(1, 16);
    registrar.Parameter("block_cache", &TThis::BlockCache)
        .Default();

    registrar.Parameter("replication_factor", &TThis::ReplicationFactor)
        .Default()
        .GreaterThan(0);
    registrar.Parameter("read_quorum", &TThis::ReadQuorum)
        .Default()
        .GreaterThan(0);
    registrar.Parameter("write_quorum", &TThis::WriteQuorum)
        .Default()
        .GreaterThan(0);
    registrar.Parameter("write_parallelism", &TThis::WriteParallelism)
        .Default()
        .InRange(1, 16);
    registrar.Parameter("chunk_maintenance_period", &TThis::ChunkMaintenancePeriod)
        .Default();
    registrar.Parameter("max_chunk_data_size", &TThis::MaxChunkDataSize)
        .Default()
        .GreaterThan(0);
    registrar.Parameter("write_backoff", &TThis::WriteBackoff)
        .Default();
    registrar.Parameter("chunk_writer", &TThis::ChunkWriter)
        .Default();
    registrar.Parameter("chunk_reader", &TThis::ChunkReader)
        .Default();

    registrar.Parameter("flush_period", &TThis::FlushPeriod)
        .Default();
    registrar.Parameter("dirty_block_pool_capacity", &TThis::DirtyBlockPoolCapacity)
        .Default()
        .GreaterThan(0);
    registrar.Parameter("dirty_fraction_threshold", &TThis::DirtyFractionThreshold)
        .Default()
        .InRange(0.0, 1.0);
}

////////////////////////////////////////////////////////////////////////////////

TJournalBlockDeviceConfigPtr TJournalBlockDeviceConfig::ApplyDynamic(
    const TJournalBlockDeviceDynamicConfigPtr& dynamicConfig) const
{
    auto config = CloneYsonStruct(MakeStrong(this));
    config->ApplyDynamicInplace(dynamicConfig);
    config->Postprocess();
    return config;
}

void TJournalBlockDeviceConfig::ApplyDynamicInplace(const TJournalBlockDeviceDynamicConfigPtr& dynamicConfig)
{
    if (!dynamicConfig) {
        return;
    }

    UpdateYsonStructField(ThreadPoolSize, dynamicConfig->ThreadPoolSize);
    if (const auto& blockCache = dynamicConfig->BlockCache) {
        UpdateYsonStructField(BlockCache->Capacity, blockCache->Capacity);
        UpdateYsonStructField(BlockCache->YoungerSizeFraction, blockCache->YoungerSizeFraction);
        UpdateYsonStructField(BlockCache->RejectOversizedItems, blockCache->RejectOversizedItems);
    }

    UpdateYsonStructField(BlockStore->ReplicationFactor, dynamicConfig->ReplicationFactor);
    UpdateYsonStructField(BlockStore->ReadQuorum, dynamicConfig->ReadQuorum);
    UpdateYsonStructField(BlockStore->WriteQuorum, dynamicConfig->WriteQuorum);
    UpdateYsonStructField(BlockStore->WriteParallelism, dynamicConfig->WriteParallelism);
    UpdateYsonStructField(BlockStore->ChunkMaintenancePeriod, dynamicConfig->ChunkMaintenancePeriod);
    UpdateYsonStructField(BlockStore->MaxChunkDataSize, dynamicConfig->MaxChunkDataSize);
    UpdateYsonStructField(BlockStore->WriteBackoff, dynamicConfig->WriteBackoff);
    if (const auto& chunkWriter = dynamicConfig->ChunkWriter) {
        BlockStore->ChunkWriter = CloneYsonStruct(chunkWriter);
    }
    if (const auto& chunkReader = dynamicConfig->ChunkReader) {
        BlockStore->ChunkReader = CloneYsonStruct(chunkReader);
    }

    UpdateYsonStructField(BlockFlusher->FlushPeriod, dynamicConfig->FlushPeriod);
    UpdateYsonStructField(BlockFlusher->DirtyBlockPoolCapacity, dynamicConfig->DirtyBlockPoolCapacity);
    UpdateYsonStructField(BlockFlusher->DirtyFractionThreshold, dynamicConfig->DirtyFractionThreshold);
}

////////////////////////////////////////////////////////////////////////////////

void TJournalBlockDeviceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("thread_pool_size", &TThis::ThreadPoolSize)
        .Default(2)
        .GreaterThan(0);
    registrar.Parameter("block_cache", &TThis::BlockCache)
        .DefaultNew();
    registrar.Parameter("block_store", &TThis::BlockStore)
        .DefaultNew();
    registrar.Parameter("block_flusher", &TThis::BlockFlusher)
        .DefaultNew();
    registrar.Parameter("block_compactor", &TThis::BlockCompactor)
        .DefaultNew();
    registrar.Parameter("snapshot_blocks_per_batch", &TThis::SnapshotBlocksPerBatch)
        .Default(1'000'000)
        .InRange(1, MaxBlocksPerDevice);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
