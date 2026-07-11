#pragma once

#include "public.h"

#include <yt/yt/server/lib/nbd/config.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/core/misc/cache_config.h>
#include <yt/yt/core/misc/config.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NNbd::NJournal {

////////////////////////////////////////////////////////////////////////////////

//! Per-device parameters of a journal-backed NBD device: its geometry and the journal chunks the
//! block store creates for it.
struct TJournalBlockDeviceOptions
    : public virtual NYTree::TYsonStruct
{
    i64 Size;
    std::string Account;
    std::string MediumName;

    REGISTER_YSON_STRUCT(TJournalBlockDeviceOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJournalBlockDeviceOptions)

////////////////////////////////////////////////////////////////////////////////

//! Configures the block store: the journal chunks backing the device.
struct TJournalBlockStoreConfig
    : public NYTree::TYsonStruct
{
    //! Replication and quorum parameters of the backing journal chunks.
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

    //! Governs the per-record write retries (each attempt targets a random writer).
    TExponentialBackoffOptions WriteBackoff;

    NApi::TJournalChunkWriterConfigPtr ChunkWriter;
    NChunkClient::TChunkFragmentReaderConfigPtr ChunkReader;

    REGISTER_YSON_STRUCT(TJournalBlockStoreConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJournalBlockStoreConfig)

////////////////////////////////////////////////////////////////////////////////

//! Configures the flusher: the in-memory dirty-block pool and its background drain.
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

//! Configures a journal-chunk-backed block device.
struct TJournalBlockDeviceConfig
    : public TBlockDeviceConfigBase
{
    //! Device block size; every read/write is aligned to it.
    i64 BlockSize;

    //! Size of the device's own thread pool, on which the store and flusher run.
    int ThreadPoolSize;

    //! Cache of clean (flushed) blocks, keyed by stored block id.
    TSlruCacheConfigPtr BlockCache;

    //! The backing store: how the journal chunks are written and read.
    TJournalBlockStoreConfigPtr BlockStore;

    //! The flusher: how dirty blocks are buffered and drained to the store.
    TJournalBlockFlusherConfigPtr BlockFlusher;

    REGISTER_YSON_STRUCT(TJournalBlockDeviceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJournalBlockDeviceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
