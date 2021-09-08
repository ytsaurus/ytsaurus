#pragma once

#include "public.h"

#include <yt/yt/client/misc/config.h>

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NChunkClient {

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

class TFetcherConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TDuration NodeRpcTimeout;

    //! If node throttled fetch request, it becomes banned for this period of time.
    TDuration NodeBanDuration;

    //! Time to sleep before next fetching round if no requests were performed.
    TDuration BackoffTime;

    int MaxChunksPerNodeFetch;

    TFetcherConfig()
    {
        RegisterParameter("node_rpc_timeout", NodeRpcTimeout)
            .Default(TDuration::Seconds(30));

        RegisterParameter("node_ban_duration", NodeBanDuration)
            .Default(TDuration::Seconds(5));

        RegisterParameter("backoff_time", BackoffTime)
            .Default(TDuration::MilliSeconds(100));

        RegisterParameter("max_chunks_per_node_fetch", MaxChunksPerNodeFetch)
            .Default(300);
    }
};

DEFINE_REFCOUNTED_TYPE(TFetcherConfig)

////////////////////////////////////////////////////////////////////////////////

class TBlockReordererConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    bool EnableBlockReordering;

    //! Instead of grouping blocks by column groups, shuffle them.
    //! Used only for testing purposes.
    bool ShuffleBlocks;

    TBlockReordererConfig()
    {
        RegisterParameter("enable_block_reordering", EnableBlockReordering)
            .Default(false);

        RegisterParameter("shuffle_blocks", ShuffleBlocks)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TBlockReordererConfig)

////////////////////////////////////////////////////////////////////////////////

class TChunkSliceFetcherConfig
    : public TFetcherConfig
{
public:
    int MaxSlicesPerFetch;

    TChunkSliceFetcherConfig()
    {
        RegisterParameter("max_slices_per_fetch", MaxSlicesPerFetch)
            .GreaterThan(0)
            .Default(10'000);
    }
};

DEFINE_REFCOUNTED_TYPE(TChunkSliceFetcherConfig)

////////////////////////////////////////////////////////////////////////////////

class TEncodingWriterConfig
    : public virtual TWorkloadConfig
    , public virtual TBlockReordererConfig
{
public:
    i64 EncodeWindowSize;
    double DefaultCompressionRatio;
    bool VerifyCompression;
    bool ComputeChecksum;
    int CompressionConcurrency;

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
        RegisterParameter("compression_concurrency", CompressionConcurrency)
            .Default(1)
            .GreaterThan(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TEncodingWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TReplicationReaderConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Timeout for a block request.
    TDuration BlockRpcTimeout;

    //! Delay before sending a hedged block request. If null then hedging is disabled.
    std::optional<TDuration> BlockRpcHedgingDelay;

    //! Whether to cancel the primary block request when backup one is sent.
    bool CancelPrimaryBlockRpcRequestOnHedging;

    //! Timeout for a lookup request.
    TDuration LookupRpcTimeout;

    //! Timeout for a meta request.
    TDuration MetaRpcTimeout;

    //! Delay before sending for a hedged meta request. If null then hedging is disabled.
    std::optional<TDuration> MetaRpcHedgingDelay;

    //! Timeout for a queue size probing request.
    TDuration ProbeRpcTimeout;

    //! Maximum number of peers to poll for queue length each round.
    int ProbePeerCount;

    //! Time to wait before asking the master for seeds.
    TDuration SeedsTimeout;

    //! Maximum number of attempts to fetch new seeds.
    int RetryCount;

    //! Fail read session immediately if master reports no seeds for chunk.
    bool FailOnNoSeeds;

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

    //! Total retry timeout, helps when we are doing too many passes.
    TDuration RetryTimeout;

    //! Total session timeout (for ReadBlocks and GetMeta calls).
    TDuration SessionTimeout;

    //! Duration between lookup requests to different peers in one pass.
    TDuration LookupSleepDuration;

    //! Number of lookup requests to single peer in one pass.
    int SinglePassIterationLimitForLookup;

    //! Number of peers processed in one pass.
    int LookupRequestPeerCount;

    //! Maximum number of passes within single retry for lookup request.
    int LookupRequestPassCount;

    //! Maximum number of retries for lookup request.
    int LookupRequestRetryCount;

    //! If |true| block cache will be accessed via asynchronous interface, if |false|
    //! synchronous interface will be used.
    bool UseAsyncBlockCache;

    //! If |true| replication reader will try to fetch blocks from local block cache.
    bool UseBlockCache;

    //! Will locate new replicas from master
    //! if node was suspicious for at least the period (unless null).
    std::optional<TDuration> SuspiciousNodeGracePeriod;

    //! Is used to increase interval between Locates
    //! that are called for discarding seeds that are suspicious.
    TDuration ProlongedDiscardSeedsDelay;

    //! If |true| GetMeta() will be performed via provided ChunkMetaCache.
    //! If ChunkMetaCache is nullptr or partition tag is specified, this option has no effect.
    bool EnableChunkMetaCache;

    //! If |true| reader will retain a set of peers that will be banned for every session. 
    bool BanPeersPermanently;

    //! For testing purposes.
    //! If |true| network throttlers will be applied even in case of requests to local host. 
    bool EnableLocalThrottling;

    TReplicationReaderConfig()
    {
        RegisterParameter("block_rpc_timeout", BlockRpcTimeout)
            .Default(TDuration::Seconds(120));
        RegisterParameter("block_rpc_hedging_delay", BlockRpcHedgingDelay)
            .Default();
        RegisterParameter("cancel_primary_block_rpc_request_on_hedging", CancelPrimaryBlockRpcRequestOnHedging)
            .Default(false);
        RegisterParameter("lookup_rpc_timeout", LookupRpcTimeout)
            .Default(TDuration::Seconds(30));
        RegisterParameter("meta_rpc_timeout", MetaRpcTimeout)
            .Default(TDuration::Seconds(30));
        RegisterParameter("meta_rpc_hedging_delay", MetaRpcHedgingDelay)
            .Default();
        RegisterParameter("probe_rpc_timeout", ProbeRpcTimeout)
            .Default(TDuration::Seconds(5));
        RegisterParameter("probe_peer_count", ProbePeerCount)
            .Default(3)
            .GreaterThan(0);
        RegisterParameter("seeds_timeout", SeedsTimeout)
            .Default(TDuration::Seconds(3));
        RegisterParameter("retry_count", RetryCount)
            .Default(20);
        RegisterParameter("fail_on_no_seeds", FailOnNoSeeds)
            .Default(false);
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
        RegisterParameter("retry_timeout", RetryTimeout)
            .Default(TDuration::Minutes(3));
        RegisterParameter("session_timeout", SessionTimeout)
            .Default(TDuration::Minutes(20));
        RegisterParameter("lookup_sleep_duration", LookupSleepDuration)
            .Default(TDuration::MilliSeconds(25));
        RegisterParameter("single_pass_iteration_limit_for_lookup", SinglePassIterationLimitForLookup)
            .Default(2);
        RegisterParameter("lookup_request_peer_count", LookupRequestPeerCount)
            .GreaterThan(0)
            .Default(5);
        RegisterParameter("lookup_request_pass_count", LookupRequestPassCount)
            .GreaterThan(0)
            .Default(10);
        RegisterParameter("lookup_request_retry_count", LookupRequestRetryCount)
            .GreaterThan(0)
            .Default(5);
        RegisterParameter("use_async_block_cache", UseAsyncBlockCache)
            .Default(false);
        RegisterParameter("use_block_cache", UseBlockCache)
            .Default(true);
        RegisterParameter("suspicious_node_grace_period", SuspiciousNodeGracePeriod)
            .Default();
        RegisterParameter("prolonged_discard_seeds_delay", ProlongedDiscardSeedsDelay)
            .Default(TDuration::Minutes(1));
        RegisterParameter("enable_chunk_meta_cache", EnableChunkMetaCache)
            .Default(true);
        RegisterParameter("ban_peers_permanently", BanPeersPermanently)
            .Default(true);
        RegisterParameter("enable_local_throttling", EnableLocalThrottling)
            .Default(false);

        RegisterPostprocessor([&] {
            // Seems unreasonable to make backoff greater than half of total session timeout.
            MaxBackoffTime = std::min(MaxBackoffTime, SessionTimeout / 2);
            RetryTimeout = std::min(RetryTimeout, SessionTimeout);

            // Rpc timeout should not exceed session timeout.
            BlockRpcTimeout = std::min(BlockRpcTimeout, RetryTimeout);
            LookupRpcTimeout = std::min(LookupRpcTimeout, RetryTimeout);
            MetaRpcTimeout = std::min(MetaRpcTimeout, RetryTimeout);
            ProbeRpcTimeout = std::min(ProbeRpcTimeout, RetryTimeout);

            // These are supposed to be not greater than PassCount and RetryCount.
            LookupRequestPassCount = std::min(LookupRequestPassCount, PassCount);
            LookupRequestRetryCount = std::min(LookupRequestRetryCount, RetryCount);
        });
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
    TDuration ReplicationReaderFailureTimeout;

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
        RegisterParameter("replication_reader_failure_timeout", ReplicationReaderFailureTimeout)
            .Default(TDuration::Minutes(10));
    }
};

DEFINE_REFCOUNTED_TYPE(TErasureReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TBlockFetcherConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Prefetch window size (in bytes).
    i64 WindowSize;

    //! Maximum amount of data to be transfered via a single RPC request.
    i64 GroupSize;

    //! If |True| block fetcher will try to fetch block from local uncompressed block cache.
    bool UseUncompressedBlockCache;

    TBlockFetcherConfig()
    {
        RegisterParameter("window_size", WindowSize)
            .Default(20_MB)
            .GreaterThan(0);
        RegisterParameter("group_size", GroupSize)
            .Default(15_MB)
            .GreaterThan(0);

        RegisterParameter("use_uncompressed_block_cache", UseUncompressedBlockCache)
            .Default(true);

        RegisterPostprocessor([&] {
            if (GroupSize > WindowSize) {
                THROW_ERROR_EXCEPTION("\"group_size\" cannot be larger than \"window_size\"");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TBlockFetcherConfig)

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkReaderConfig
    : public virtual TErasureReaderConfig
    , public virtual TBlockFetcherConfig
    , public virtual TFetchChunkSpecConfig
    , public virtual TWorkloadConfig
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

class TReplicationWriterConfig
    : public virtual TWorkloadConfig
    , public virtual TBlockReordererConfig
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

    std::optional<int> DirectUploadNodeCount;

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

    std::optional<TDuration> TestingDelay;

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
            .Default(true);
        RegisterParameter("min_upload_replication_factor", MinUploadReplicationFactor)
            .Default(2)
            .GreaterThanOrEqual(1);
        RegisterParameter("direct_upload_node_count", DirectUploadNodeCount)
            .Default();
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

        RegisterParameter("testing_delay", TestingDelay)
            .Default();

        RegisterPreprocessor([&] () {
            NodeChannel->RetryBackoffTime = TDuration::Seconds(10);
            NodeChannel->RetryAttempts = 100;
        });

        RegisterPostprocessor([&] {
            if (!DirectUploadNodeCount) {
                return;
            }

            if (*DirectUploadNodeCount < 1) {
                THROW_ERROR_EXCEPTION("\"direct_upload_node_count\" cannot be less that 1");
            }
        });

        RegisterPostprocessor([&] () {
            if (SendWindowSize < GroupSize) {
                THROW_ERROR_EXCEPTION("\"send_window_size\" cannot be less than \"group_size\"");
            }
        });
    }

    int GetDirectUploadNodeCount()
    {
        auto replicationFactor = std::min(MinUploadReplicationFactor, UploadReplicationFactor);
        if (DirectUploadNodeCount) {
            return std::min(*DirectUploadNodeCount, replicationFactor);
        }

        return std::max(static_cast<int>(std::sqrt(replicationFactor)), 1);
    }
};

DEFINE_REFCOUNTED_TYPE(TReplicationWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TErasureWriterConfig
    : public virtual NYTree::TYsonSerializable
    , public virtual TBlockReordererConfig
{
public:
    i64 ErasureWindowSize;
    std::optional<i64> ErasureStripeSize;

    bool EnableErasureTargetNodeReallocation;
    bool ErasureStoreOriginalBlockChecksums;

    TErasureWriterConfig()
    {
        RegisterParameter("enable_erasure_target_node_reallocation", EnableErasureTargetNodeReallocation)
            .Default(false);

        RegisterParameter("erasure_window_size", ErasureWindowSize)
            .Default(8_MB)
            .GreaterThan(0);

        RegisterParameter("erasure_store_original_block_checksums", ErasureStoreOriginalBlockChecksums)
            .Default(false);

        RegisterParameter("erasure_stripe_size", ErasureStripeSize)
            .Default()
            .GreaterThan(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TErasureWriterConfig)

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

} // namespace NYT::NChunkClient
