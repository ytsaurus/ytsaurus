#pragma once

#include "public.h"
#include "chunk_replica.h"

#include <yt/ytlib/misc/config.h>
#include <yt/ytlib/misc/workload.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/api/public.h>

#include <yt/core/compression/public.h>

#include <yt/core/erasure/public.h>

#include <yt/core/misc/config.h>
#include <yt/core/misc/error.h>

#include <yt/core/rpc/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TReplicationReaderConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Timeout for a block request.
    TDuration BlockRpcTimeout;

    //! Timeout for a meta request.
    TDuration MetaRpcTimeout;

    //! Timeout for a queue size probing request.
    TDuration ProbeRpcTimeout;

    //! Maximum number of peers to poll for queue length each round.
    int ProbePeerCount;

    //! Time to wait before asking the master for seeds.
    TDuration SeedsTimeout;

    //! Maximum number of attempts to fetch new seeds.
    int RetryCount;

    //! Time to wait before making another pass with same seeds.
    //! Increases exponentially with every pass, from MinPassBackoffTime to MaxPassBackoffTime.
    TDuration MinBackoffTime;
    TDuration MaxBackoffTime;
    double BackoffTimeMultiplier;

    //! Maximum number of passes with same seeds.
    int PassCount;

    //! Enable fetching blocks from peers suggested by seeds.
    bool FetchFromPeers;

    //! Timeout after which a node forgets about the peer.
    //! Only makes sense if the reader is equipped with peer descriptor.
    TDuration PeerExpirationTimeout;

    //! If |true| then fetched blocks are cached by the node.
    bool PopulateCache;

    //! If |true| then local data center replicas are unconditionally preferred to remote replicas.
    bool PreferLocalDataCenter;

    //! If |true| then local rack replicas are unconditionally preferred to remote replicas.
    bool PreferLocalRack;

    //! If |true| then local host replicas are unconditionally preferred to any other replicas.
    bool PreferLocalHost;

    //! If peer ban counter exceeds #MaxBanCount, peer is banned forever.
    int MaxBanCount;

    //! Factors to calculate peer load as linear combination of disk queue and net queue.
    double NetQueueSizeFactor;
    double DiskQueueSizeFactor;

    //! If |true|, then workload descriptors are annotated with the read session start time
    //! and are thus scheduled in FIFO order.
    bool EnableWorkloadFifoScheduling;

    TReplicationReaderConfig()
    {
        RegisterParameter("block_rpc_timeout", BlockRpcTimeout)
            .Default(TDuration::Seconds(120));
        RegisterParameter("meta_rpc_timeout", MetaRpcTimeout)
            .Default(TDuration::Seconds(30));
        RegisterParameter("probe_rpc_timeout", ProbeRpcTimeout)
            .Default(TDuration::Seconds(5));
        RegisterParameter("probe_peer_count", ProbePeerCount)
            .Default(3)
            .GreaterThan(0);
        RegisterParameter("seeds_timeout", SeedsTimeout)
            .Default(TDuration::Seconds(3));
        RegisterParameter("retry_count", RetryCount)
            .Default(20);
        RegisterParameter("min_backoff_time", MinBackoffTime)
            .Default(TDuration::Seconds(3));
        RegisterParameter("max_backoff_time", MaxBackoffTime)
            .Default(TDuration::Seconds(60));
        RegisterParameter("backoff_time_multiplier", BackoffTimeMultiplier)
            .GreaterThan(1)
            .Default(1.5);
        RegisterParameter("pass_count", PassCount)
            .Default(500);
        RegisterParameter("fetch_from_peers", FetchFromPeers)
            .Default(true);
        RegisterParameter("peer_expiration_timeout", PeerExpirationTimeout)
            .Default(TDuration::Seconds(300));
        RegisterParameter("populate_cache", PopulateCache)
            .Default(true);
        RegisterParameter("prefer_local_host", PreferLocalHost)
            .Default(false);
        RegisterParameter("prefer_local_rack", PreferLocalRack)
            .Default(false);
        RegisterParameter("prefer_local_data_center", PreferLocalDataCenter)
            .Default(true);
        RegisterParameter("max_ban_count", MaxBanCount)
            .Default(5);
        RegisterParameter("disk_queue_size_factor", DiskQueueSizeFactor)
            .Default(1.0);
        RegisterParameter("net_queue_size_factor", NetQueueSizeFactor)
            .Default(0.5);
        RegisterParameter("enable_workload_fifo_scheduling", EnableWorkloadFifoScheduling)
            .Default(true);
    }
};

DEFINE_REFCOUNTED_TYPE(TReplicationReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TErasureReaderConfig
    : public TReplicationReaderConfig
{
public:
    bool EnableAutoRepair;
    double ReplicationReaderSpeedLimitPerSec;
    TDuration SlowReaderExpirationTimeout;
    TDuration ReplicationReaderTimeout;

    TErasureReaderConfig()
    {
        RegisterParameter("enable_auto_repair", EnableAutoRepair)
            .Default(true);
        RegisterParameter("replication_reader_speed_limit_per_sec", ReplicationReaderSpeedLimitPerSec)
            .Default(5_MB);
        RegisterParameter("slow_reader_expiration_timeout", SlowReaderExpirationTimeout)
            .Default(TDuration::Minutes(2));
        RegisterParameter("replication_reader_timeout", ReplicationReaderTimeout)
            .Default(TDuration::Seconds(60));
    }
};

DEFINE_REFCOUNTED_TYPE(TErasureReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TRemoteReaderOptions
    : public virtual NYTree::TYsonSerializable
{
public:
    //! If |true| then the master may be asked for seeds.
    bool AllowFetchingSeedsFromMaster;

    //! Advertise current host as a P2P peer.
    bool EnableP2P;

    TRemoteReaderOptions()
    {
        RegisterParameter("allow_fetching_seeds_from_master", AllowFetchingSeedsFromMaster)
            .Default(true);

        RegisterParameter("enable_p2p", EnableP2P)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TRemoteReaderOptions)

////////////////////////////////////////////////////////////////////////////////

class TBlockFetcherConfig
    : public virtual TWorkloadConfig
{
public:
    //! Prefetch window size (in bytes).
    i64 WindowSize;

    //! Maximum amount of data to be transfered via a single RPC request.
    i64 GroupSize;

    TBlockFetcherConfig()
    {
        RegisterParameter("window_size", WindowSize)
            .Default(20_MB)
            .GreaterThan(0);
        RegisterParameter("group_size", GroupSize)
            .Default(15_MB)
            .GreaterThan(0);

        RegisterPostprocessor([&] () {
            if (GroupSize > WindowSize) {
                THROW_ERROR_EXCEPTION("\"group_size\" cannot be larger than \"window_size\"");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TBlockFetcherConfig)

////////////////////////////////////////////////////////////////////////////////

class TReplicationWriterConfig
    : public virtual TWorkloadConfig
{
public:
    //! Maximum window size (in bytes).
    i64 SendWindowSize;

    //! Maximum group size (in bytes).
    i64 GroupSize;

    //! RPC requests timeout.
    /*!
     *  This timeout is especially useful for |PutBlocks| calls to ensure that
     *  uploading is not stalled.
     */
    TDuration NodeRpcTimeout;

    NRpc::TRetryingChannelConfigPtr NodeChannel;

    int UploadReplicationFactor;

    int MinUploadReplicationFactor;

    bool PreferLocalHost;

    bool BanFailedNodes;

    //! Interval between consecutive pings to Data Nodes.
    TDuration NodePingPeriod;

    //! If |true| then written blocks are cached by the node.
    bool PopulateCache;

    //! If |true| then the chunk is fsynced to disk upon closing.
    bool SyncOnClose;

    bool EnableDirectIO;

    //! If |true| then the chunk is finished as soon as MinUploadReplicationFactor chunks are written.
    bool EnableEarlyFinish;

    TDuration AllocateWriteTargetsBackoffTime;

    int AllocateWriteTargetsRetryCount;

    TReplicationWriterConfig()
    {
        RegisterParameter("send_window_size", SendWindowSize)
            .Default(32_MB)
            .GreaterThan(0);
        RegisterParameter("group_size", GroupSize)
            .Default(10_MB)
            .GreaterThan(0);
        RegisterParameter("node_channel", NodeChannel)
            .DefaultNew();
        RegisterParameter("node_rpc_timeout", NodeRpcTimeout)
            .Default(TDuration::Seconds(300));
        RegisterParameter("upload_replication_factor", UploadReplicationFactor)
            .GreaterThanOrEqual(1)
            .Default(2);
        RegisterParameter("ban_failed_nodes", BanFailedNodes)
            .Default(false);
        RegisterParameter("min_upload_replication_factor", MinUploadReplicationFactor)
            .Default(2)
            .GreaterThanOrEqual(1);
        RegisterParameter("prefer_local_host", PreferLocalHost)
            .Default(true);
        RegisterParameter("node_ping_interval", NodePingPeriod)
            .Default(TDuration::Seconds(10));
        RegisterParameter("populate_cache", PopulateCache)
            .Default(false);
        RegisterParameter("sync_on_close", SyncOnClose)
            .Default(true);
        RegisterParameter("enable_direct_io", EnableDirectIO)
            .Default(false);
        RegisterParameter("enable_early_finish", EnableEarlyFinish)
            .Default(false);
        RegisterParameter("allocate_write_targets_backoff_time", AllocateWriteTargetsBackoffTime)
            .Default(TDuration::Seconds(5));
        RegisterParameter("allocate_write_targets_retry_count", AllocateWriteTargetsRetryCount)
            .Default(10);

        RegisterPreprocessor([&] () {
            NodeChannel->RetryBackoffTime = TDuration::Seconds(10);
            NodeChannel->RetryAttempts = 100;
        });

        RegisterPostprocessor([&] () {
            if (SendWindowSize < GroupSize) {
                THROW_ERROR_EXCEPTION("\"send_window_size\" cannot be less than \"group_size\"");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TReplicationWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TRemoteWriterOptions
    : public virtual NYTree::TYsonSerializable
{
public:
    bool AllowAllocatingNewTargetNodes;
    TString MediumName;
    TPlacementId PlacementId;

    TRemoteWriterOptions()
    {
        RegisterParameter("allow_allocating_new_target_nodes", AllowAllocatingNewTargetNodes)
            .Default(true);
        RegisterParameter("medium_name", MediumName)
            .Default(DefaultStoreMediumName);
        RegisterParameter("placement_id", PlacementId)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TRemoteWriterOptions)

////////////////////////////////////////////////////////////////////////////////

class TErasureWriterConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    i64 ErasureWindowSize;

    bool EnableErasureTargetNodeReallocation;

    TErasureWriterConfig()
    {
        RegisterParameter("enable_erasure_target_node_reallocation", EnableErasureTargetNodeReallocation)
            .Default(false);

        RegisterParameter("erasure_window_size", ErasureWindowSize)
            .Default(8_MB)
            .GreaterThan(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TErasureWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TEncodingWriterConfig
    : public virtual TWorkloadConfig
{
public:
    i64 EncodeWindowSize;
    double DefaultCompressionRatio;
    bool VerifyCompression;
    bool ComputeChecksum;

    TEncodingWriterConfig()
    {
        RegisterParameter("encode_window_size", EncodeWindowSize)
            .Default(16_MB)
            .GreaterThan(0);
        RegisterParameter("default_compression_ratio", DefaultCompressionRatio)
            .Default(0.2);
        RegisterParameter("verify_compression", VerifyCompression)
            .Default(true);
        RegisterParameter("compute_checksum", ComputeChecksum)
            .Default(true);
    }
};

DEFINE_REFCOUNTED_TYPE(TEncodingWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TEncodingWriterOptions
    : public virtual NYTree::TYsonSerializable
{
public:
    NCompression::ECodec CompressionCodec;
    bool ChunksEden;

    TEncodingWriterOptions()
    {
        RegisterParameter("compression_codec", CompressionCodec)
            .Default(NCompression::ECodec::None);
        RegisterParameter("chunks_eden", ChunksEden)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TEncodingWriterOptions)

////////////////////////////////////////////////////////////////////////////////

class TDispatcherConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    int CompressionPoolSize;
    int ErasurePoolSize;

    TDispatcherConfig()
    {
        RegisterParameter("compression_pool_size", CompressionPoolSize)
            .Default(4)
            .GreaterThan(0);
        RegisterParameter("erasure_pool_size", ErasurePoolSize)
            .Default(4)
            .GreaterThan(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TDispatcherConfig)

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkWriterConfig
    : public TReplicationWriterConfig
    , public TErasureWriterConfig
{
public:
    i64 DesiredChunkSize;
    i64 DesiredChunkWeight;
    i64 MaxMetaSize;

    TMultiChunkWriterConfig()
    {
        RegisterParameter("desired_chunk_size", DesiredChunkSize)
            .GreaterThan(0)
            .Default(2_GB);

        RegisterParameter("desired_chunk_weight", DesiredChunkWeight)
            .GreaterThan(0)
            .Default(100_GB);

        RegisterParameter("max_meta_size", MaxMetaSize)
            .GreaterThan(0)
            .LessThanOrEqual(64_MB)
            .Default(30_MB);
    }
};

DEFINE_REFCOUNTED_TYPE(TMultiChunkWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkWriterOptions
    : public virtual TEncodingWriterOptions
    , public virtual TRemoteWriterOptions
{
public:
    int ReplicationFactor;
    TString Account;
    bool ChunksVital;
    bool ChunksMovable;
    bool ValidateResourceUsageIncrease;

    //! This field doesn't affect the behavior of writer.
    //! It is stored in table_index field of output_chunk_specs.
    int TableIndex;

    NErasure::ECodec ErasureCodec;

    TMultiChunkWriterOptions()
    {
        RegisterParameter("replication_factor", ReplicationFactor)
            .GreaterThanOrEqual(1)
            .Default(DefaultReplicationFactor);
        RegisterParameter("account", Account)
            .NonEmpty();
        RegisterParameter("chunks_vital", ChunksVital)
            .Default(true);
        RegisterParameter("chunks_movable", ChunksMovable)
            .Default(true);
        RegisterParameter("validate_resource_usage_increase", ValidateResourceUsageIncrease)
            .Default(true);
        RegisterParameter("erasure_codec", ErasureCodec)
            .Default(NErasure::ECodec::None);
        RegisterParameter("table_index", TableIndex)
            .Default(-1);
    }
};

DEFINE_REFCOUNTED_TYPE(TMultiChunkWriterOptions)

////////////////////////////////////////////////////////////////////////////////

class TFetchChunkSpecConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    int MaxChunksPerFetch;
    int MaxChunksPerLocateRequest;

    TFetchChunkSpecConfig()
    {
        RegisterParameter("max_chunks_per_fetch", MaxChunksPerFetch)
            .GreaterThan(0)
            .Default(100000);
        RegisterParameter("max_chunks_per_locate_request", MaxChunksPerLocateRequest)
            .GreaterThan(0)
            .Default(10000);
    }
};

DEFINE_REFCOUNTED_TYPE(TFetchChunkSpecConfig)

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkReaderConfig
    : public virtual TErasureReaderConfig
    , public virtual TBlockFetcherConfig
    , public virtual TFetchChunkSpecConfig
{
public:
    i64 MaxBufferSize;
    int MaxParallelReaders;

    TMultiChunkReaderConfig()
    {
        RegisterParameter("max_buffer_size", MaxBufferSize)
            .GreaterThan(0L)
            .LessThanOrEqual(10_GB)
            .Default(100_MB);
        RegisterParameter("max_parallel_readers", MaxParallelReaders)
            .GreaterThanOrEqual(1)
            .LessThanOrEqual(1000)
            .Default(512);

        RegisterPostprocessor([&] () {
            if (MaxBufferSize < 2 * WindowSize) {
                THROW_ERROR_EXCEPTION("\"max_buffer_size\" cannot be less than twice \"window_size\"");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TMultiChunkReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkReaderOptions
    : public TRemoteReaderOptions
{
public:
    bool KeepInMemory;

    TMultiChunkReaderOptions()
    {
        RegisterParameter("keep_in_memory", KeepInMemory)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TMultiChunkReaderOptions)

////////////////////////////////////////////////////////////////////////////////

class TFetcherConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    NRpc::TRetryingChannelConfigPtr NodeChannel;

    TDuration NodeRpcTimeout;

    TFetcherConfig()
    {
        RegisterParameter("node_channel", NodeChannel)
            .DefaultNew();
        RegisterParameter("node_rpc_timeout", NodeRpcTimeout)
            .Default(TDuration::Seconds(30));
    }
};

DEFINE_REFCOUNTED_TYPE(TFetcherConfig)

////////////////////////////////////////////////////////////////////////////////

class TBlockCacheConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TSlruCacheConfigPtr CompressedData;
    TSlruCacheConfigPtr UncompressedData;

    i64 GetTotalCapacity() const
    {
        return
            CompressedData->Capacity +
            UncompressedData->Capacity;
    }

    TBlockCacheConfig()
    {
        RegisterParameter("compressed_data", CompressedData)
            .DefaultNew();
        RegisterParameter("uncompressed_data", UncompressedData)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TBlockCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TChunkScraperConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Number of chunks scratched per one LocateChunks.
    int MaxChunksPerRequest;

    TChunkScraperConfig()
    {
        RegisterParameter("max_chunks_per_request", MaxChunksPerRequest)
            .Default(1000)
            .GreaterThan(0)
            .LessThan(100000);
    }
};

DEFINE_REFCOUNTED_TYPE(TChunkScraperConfig)

////////////////////////////////////////////////////////////////////////////////

class TChunkTeleporterConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Maximum number of chunks to export/import per request.
    int MaxTeleportChunksPerRequest;

    TChunkTeleporterConfig()
    {
        RegisterParameter("max_teleport_chunks_per_request", MaxTeleportChunksPerRequest)
            .GreaterThan(0)
            .Default(5000);
    }
};

DEFINE_REFCOUNTED_TYPE(TChunkTeleporterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
