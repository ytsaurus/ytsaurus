#pragma once

#include "public.h"

#include <yt/yt/server/lib/nbd/config.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/cache_config.h>
#include <yt/yt/core/misc/config.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NNbd::NJournal {

////////////////////////////////////////////////////////////////////////////////

struct TJournalBlockDeviceOptions
    : public NYTree::TYsonStruct
{
    i64 DeviceSize = 0;
    i64 BlockSize = 0;

    std::string Account;
    std::string MediumName;

    REGISTER_YSON_STRUCT(TJournalBlockDeviceOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJournalBlockDeviceOptions)

////////////////////////////////////////////////////////////////////////////////

struct TJournalBlockStoreConfig
    : public NYTree::TYsonStruct
{
    int ReplicationFactor;
    int ReadQuorum;
    int WriteQuorum;

    //! Number of journal chunks kept open for writing simultaneously (write fan-out).
    int WriteParallelism;

    //! How often the background executor retires oversized chunks and tops up the writable set.
    TDuration ChunkMaintenancePeriod;

    //! Soft upper bound on the amount of data written to a single journal chunk.
    //! A writer is retired once its chunk grows past this size.
    i64 MaxChunkDataSize;

    //! How long a fully-dead chunk (sealed with every block superseded) is retained before it is
    //! unstaged. The delay outlives any in-flight read or write of the chunk, so freeing it cannot turn
    //! such an operation into a failure.
    TDuration DeadChunkRetentionDelay;

    //! Governs the per-record write retries (each attempt targets a random writer).
    TExponentialBackoffOptions WriteBackoff;

    //! Governs the retries when creating (topping up) a writable journal chunk. Once these are
    //! exhausted the store fails, so a persistent creation failure does not retry forever.
    TExponentialBackoffOptions ChunkCreationBackoff;

    //! Paces the retries when sealing an abandoned chunk. Sealing must eventually succeed -- a snapshot
    //! cannot reference an unsealed chunk -- so these retries are unbounded.
    TExponentialBackoffOptions SealBackoff;

    //! Timeouts for the per-replica requests backing a seal (session abort and quorum probe).
    TDuration SealRpcTimeout;
    TDuration SealQuorumSessionDelay;

    //! How long a snapshot waits for the chunks it references to be sealed.
    TDuration SnapshotSealTimeout;

    //! How long a snapshot waits for its pre-snapshot dirty blocks to reach the store.
    TDuration SnapshotFlushTimeout;

    NApi::TJournalChunkWriterConfigPtr ChunkWriter;
    NChunkClient::TChunkFragmentReaderConfigPtr ChunkReader;

    TAdaptiveHedgingManagerConfigPtr ReadHedgingManager;

    REGISTER_YSON_STRUCT(TJournalBlockStoreConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJournalBlockStoreConfig)

////////////////////////////////////////////////////////////////////////////////

struct TJournalBlockFlusherConfig
    : public NYTree::TYsonStruct
{
    //! How often the flusher moves dirty blocks from the pool to the store.
    TDuration FlushPeriod;

    //! Maximum total size, in bytes, of the in-memory pool buffering dirty (written but not yet
    //! flushed) blocks; writes back-pressure once it fills up.
    i64 DirtyBlockPoolCapacity;

    //! Fraction of #DirtyBlockPoolCapacity the flusher drains the pool down to; the dirty block
    //! count is kept around this level.
    double DirtyFractionThreshold;

    REGISTER_YSON_STRUCT(TJournalBlockFlusherConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJournalBlockFlusherConfig)

////////////////////////////////////////////////////////////////////////////////

//! Background compaction: relocates the surviving blocks of a mostly-dead retired chunk into fresh
//! chunks so the old one becomes fully dead and is reclaimed, defragmenting the store over time.
struct TJournalBlockCompactorConfig
    : public NYTree::TYsonStruct
{
    //! A retired chunk is compacted once its garbage ratio (superseded blocks over blocks ever written)
    //! reaches this.
    double GarbageRatioThreshold;

    //! How often the compactor looks for a chunk to compact, when the previous scan succeeded.
    TDuration ScanPeriod;

    //! Upper bound on chunks compacted concurrently; each scan starts at most one.
    int MaxConcurrentCompactions;

    //! Paces the retries after a failed compaction, in place of #ScanPeriod until one succeeds.
    TExponentialBackoffOptions Backoff;

    //! Upper bound on the blocks relocated per read-write-remap round; caps a round's memory footprint.
    int MaxBlocksPerBatch;

    //! Paces the total bytes compaction reads and rewrites, in bytes per second. An unset limit means
    //! unlimited.
    NConcurrency::TThroughputThrottlerConfigPtr ThroughputThrottler;

    REGISTER_YSON_STRUCT(TJournalBlockCompactorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJournalBlockCompactorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TJournalBlockDeviceConfig
    : public TBlockDeviceConfigBase
{
    //! Size of the device's own thread pool, on which the store and flusher run.
    int ThreadPoolSize;

    //! Cache of clean (flushed) blocks, keyed by stored block id.
    TSlruCacheConfigPtr BlockCache;

    //! The backing store: how the journal chunks are written and read.
    TJournalBlockStoreConfigPtr BlockStore;

    //! The flusher: how dirty blocks are buffered and drained to the store.
    TJournalBlockFlusherConfigPtr BlockFlusher;

    //! Background compaction of mostly-dead chunks; absent disables it.
    TJournalBlockCompactorConfigPtr BlockCompactor;

    //! How wide a block index window a snapshot scans, resolves and writes at a time. Caps the save's
    //! peak memory, which would otherwise scale with the device's block count.
    int SnapshotBlocksPerBatch;

    REGISTER_YSON_STRUCT(TJournalBlockDeviceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJournalBlockDeviceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
