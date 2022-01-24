#include "config.h"

#include <yt/yt/core/rpc/config.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

TFetchChunkSpecConfig::TFetchChunkSpecConfig()
{
    RegisterParameter("max_chunks_per_fetch", MaxChunksPerFetch)
        .GreaterThan(0)
        .Default(100000);
    RegisterParameter("max_chunks_per_locate_request", MaxChunksPerLocateRequest)
        .GreaterThan(0)
        .Default(10000);
}

////////////////////////////////////////////////////////////////////////////////

TFetcherConfig::TFetcherConfig()
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

////////////////////////////////////////////////////////////////////////////////

TBlockReordererConfig::TBlockReordererConfig()
{
    RegisterParameter("enable_block_reordering", EnableBlockReordering)
        .Default(false);

    RegisterParameter("shuffle_blocks", ShuffleBlocks)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

TChunkSliceFetcherConfig::TChunkSliceFetcherConfig()
{
    RegisterParameter("max_slices_per_fetch", MaxSlicesPerFetch)
        .GreaterThan(0)
        .Default(10'000);
}

////////////////////////////////////////////////////////////////////////////////

TEncodingWriterConfig::TEncodingWriterConfig()
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

////////////////////////////////////////////////////////////////////////////////

TReplicationReaderConfig::TReplicationReaderConfig()
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

////////////////////////////////////////////////////////////////////////////////

TErasureReaderConfig::TErasureReaderConfig()
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

////////////////////////////////////////////////////////////////////////////////

TBlockFetcherConfig::TBlockFetcherConfig()
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

////////////////////////////////////////////////////////////////////////////////

TMultiChunkReaderConfig::TMultiChunkReaderConfig()
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

////////////////////////////////////////////////////////////////////////////////

TReplicationWriterConfig::TReplicationWriterConfig()
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

int TReplicationWriterConfig::GetDirectUploadNodeCount()
{
    auto replicationFactor = std::min(MinUploadReplicationFactor, UploadReplicationFactor);
    if (DirectUploadNodeCount) {
        return std::min(*DirectUploadNodeCount, replicationFactor);
    }

    return std::max(static_cast<int>(std::sqrt(replicationFactor)), 1);
}

////////////////////////////////////////////////////////////////////////////////

TErasureWriterConfig::TErasureWriterConfig()
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

////////////////////////////////////////////////////////////////////////////////

TMultiChunkWriterConfig::TMultiChunkWriterConfig()
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

////////////////////////////////////////////////////////////////////////////////

TEncodingWriterOptions::TEncodingWriterOptions()
{
    RegisterParameter("compression_codec", CompressionCodec)
        .Default(NCompression::ECodec::None);
    RegisterParameter("chunks_eden", ChunksEden)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
