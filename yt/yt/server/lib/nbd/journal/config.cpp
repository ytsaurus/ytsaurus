#include "config.h"

#include <yt/yt/ytlib/journal_client/helpers.h>

#include <yt/yt/client/api/config.h>

#include <yt/yt/client/chunk_client/config.h>

#include <yt/yt/library/erasure/public.h>

#include <util/generic/bitops.h>

namespace NYT::NNbd::NJournal {

////////////////////////////////////////////////////////////////////////////////

void TJournalBlockDeviceOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("size", &TThis::Size)
        .GreaterThanOrEqual(0);
    registrar.Parameter("account", &TThis::Account);
    registrar.Parameter("medium_name", &TThis::MediumName);
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
        .Default(1_GB)
        .GreaterThan(0);
    registrar.Parameter("write_backoff", &TThis::WriteBackoff)
        .Default(TExponentialBackoffOptions{
            .InvocationCount = 10,
            .MinBackoff = TDuration::MilliSeconds(100),
            .MaxBackoff = TDuration::Seconds(3),
        });
    registrar.Parameter("chunk_writer", &TThis::ChunkWriter)
        .DefaultNew();
    registrar.Parameter("chunk_reader", &TThis::ChunkReader)
        .DefaultNew();

    registrar.Postprocessor([] (TThis* config) {
        NJournalClient::ValidateJournalAttributes(
            NErasure::ECodec::None,
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

void TJournalBlockDeviceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("block_size", &TThis::BlockSize)
        .Default(4_KB)
        .GreaterThan(0);
    registrar.Parameter("thread_pool_size", &TThis::ThreadPoolSize)
        .Default(2)
        .GreaterThan(0);
    registrar.Parameter("block_cache", &TThis::BlockCache)
        .DefaultNew();
    registrar.Parameter("block_store", &TThis::BlockStore)
        .DefaultNew();
    registrar.Parameter("block_flusher", &TThis::BlockFlusher)
        .DefaultNew();

    registrar.Postprocessor([] (TThis* config) {
        if (!IsPowerOf2(config->BlockSize)) {
            THROW_ERROR_EXCEPTION("\"block_size\" must be a power of two")
                << TErrorAttribute("block_size", config->BlockSize);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
