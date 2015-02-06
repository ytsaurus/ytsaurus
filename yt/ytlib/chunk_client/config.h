#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/ytree/yson_serializable.h>

#include <core/compression/public.h>
#include <core/erasure/public.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

class TReplicationReaderConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Timeout for a block request.
    TDuration BlockRpcTimeout;

    //! Timeout for a meta request.
    TDuration MetaRpcTimeout;

    //! Time to wait before asking the master for seeds.
    TDuration RetryBackoffTime;

    //! Maximum number of attempts to fetch new seeds.
    int RetryCount;

    //! Time to wait before making another pass with same seeds.
    //! Increases exponentially with every pass, from MinPassBackoffTime to MaxPassBackoffTime.
    TDuration MinPassBackoffTime;
    TDuration MaxPassBackoffTime;
    double PassBackoffTimeMultiplier;

    //! Maximum number of passes with same seeds.
    int PassCount;

    //! Enable fetching blocks from peers suggested by seeds.
    bool FetchFromPeers;

    //! Timeout after which a node forgets about the peer.
    //! Only makes sense if the reader is equipped with peer descriptor.
    TDuration PeerExpirationTimeout;

    //! If True then fetched blocks are cached by the node.
    bool EnableNodeCaching;

    //! If True then the master may be asked for seeds.
    bool AllowFetchingSeedsFromMaster;

    TReplicationReaderConfig()
    {
        RegisterParameter("block_rpc_timeout", BlockRpcTimeout)
            .Default(TDuration::Seconds(120));
        RegisterParameter("meta_rpc_timeout", MetaRpcTimeout)
            .Default(TDuration::Seconds(30));
        RegisterParameter("retry_backoff_time", RetryBackoffTime)
            .Default(TDuration::Seconds(3));
        RegisterParameter("retry_count", RetryCount)
            .Default(20);
        RegisterParameter("min_pass_backoff_time", MinPassBackoffTime)
            .Default(TDuration::Seconds(3));
        RegisterParameter("max_pass_backoff_time", MaxPassBackoffTime)
            .Default(TDuration::Seconds(60));
        RegisterParameter("pass_backoff_time_multiplier", PassBackoffTimeMultiplier)
            .GreaterThan(1)
            .Default(1.5);
        RegisterParameter("pass_count", PassCount)
            .Default(500);
        RegisterParameter("fetch_from_peers", FetchFromPeers)
            .Default(true);
        RegisterParameter("peer_expiration_timeout", PeerExpirationTimeout)
            .Default(TDuration::Seconds(300));
        RegisterParameter("enable_node_caching", EnableNodeCaching)
            .Default(true);
        RegisterParameter("allow_fetching_seeds_from_master", AllowFetchingSeedsFromMaster)
            .Default(true);
    }
};

///////////////////////////////////////////////////////////////////////////////

class TClientBlockCacheConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! The maximum number of bytes that block are allowed to occupy.
    //! Zero means that no blocks are cached.
    i64 MaxSize;

    TClientBlockCacheConfig()
    {
        RegisterParameter("max_size", MaxSize)
            .Default(0)
            .GreaterThanOrEqual(0);
    }
};

///////////////////////////////////////////////////////////////////////////////

class TSequentialReaderConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Prefetch window size (in bytes).
    i64 WindowSize;

    //! Maximum amount of data to be transfered via a single RPC request.
    i64 GroupSize;

    TSequentialReaderConfig()
    {
        RegisterParameter("window_size", WindowSize)
            .Default((i64) 20 * 1024 * 1024)
            .GreaterThan(0);
        RegisterParameter("group_size", GroupSize)
            .Default((i64) 15 * 1024 * 1024)
            .GreaterThan(0);

        RegisterValidator([&] () {
            if (GroupSize > WindowSize) {
                THROW_ERROR_EXCEPTION("\"group_size\" cannot be larger than \"window_size\"");
            }
        });
    }
};

///////////////////////////////////////////////////////////////////////////////

class TReplicationWriterConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Maximum window size (in bytes).
    i64 SendWindowSize;

    //! Maximum group size (in bytes).
    i64 GroupSize;

    //! RPC requests timeout.
    /*!
     *  This timeout is especially useful for PutBlocks calls to ensure that
     *  uploading is not stalled.
     */
    TDuration NodeRpcTimeout;

    int MinUploadReplicationFactor;

    //! Maximum allowed period of time without RPC requests to nodes.
    /*!
     *  If the writer remains inactive for the given period, it sends #TChunkHolderProxy::PingSession.
     */
    TDuration NodePingInterval;

    //! If True then written blocks are cached by the node.
    bool EnableNodeCaching;

    bool SyncOnClose;

    TReplicationWriterConfig()
    {
        RegisterParameter("send_window_size", SendWindowSize)
            .Default((i64) 32 * 1024 * 1024)
            .GreaterThan(0);
        RegisterParameter("group_size", GroupSize)
            .Default((i64) 10 * 1024 * 1024)
            .GreaterThan(0);
        RegisterParameter("node_rpc_timeout", NodeRpcTimeout)
            .Default(TDuration::Seconds(120));
        RegisterParameter("min_upload_replication_factor", MinUploadReplicationFactor)
            .Default(2)
            .GreaterThan(0);
        RegisterParameter("node_ping_interval", NodePingInterval)
            .Default(TDuration::Seconds(10));
        RegisterParameter("enable_node_caching", EnableNodeCaching)
            .Default(false);
        RegisterParameter("sync_on_close", SyncOnClose)
            .Default(true);

        RegisterValidator([&] () {
            if (SendWindowSize < GroupSize) {
                THROW_ERROR_EXCEPTION("\"send_window_size\" cannot be less than \"group_size\"");
            }
        });
    }
};

///////////////////////////////////////////////////////////////////////////////

class TErasureWriterConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    i64 ErasureWindowSize;

    TErasureWriterConfig()
    {
        RegisterParameter("erasure_window_size", ErasureWindowSize)
            .Default((i64)8 * 1024 * 1024)
            .GreaterThan(0);
    }
};

///////////////////////////////////////////////////////////////////////////////

class TEncodingWriterConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    i64 EncodeWindowSize;
    double DefaultCompressionRatio;
    bool VerifyCompression;

    TEncodingWriterConfig()
    {
        RegisterParameter("encode_window_size", EncodeWindowSize)
            .Default((i64) 16 * 1024 * 1024)
            .GreaterThan(0);
        RegisterParameter("default_compression_ratio", DefaultCompressionRatio)
            .Default(0.2);
        RegisterParameter("verify_compression", VerifyCompression)
            .Default(true);
    }
};

///////////////////////////////////////////////////////////////////////////////

struct TEncodingWriterOptions
    : public virtual NYTree::TYsonSerializable
{
    NCompression::ECodec CompressionCodec;

    TEncodingWriterOptions()
    {
        RegisterParameter("compression_codec", CompressionCodec)
            .Default(NCompression::ECodec::None);
    }
};

///////////////////////////////////////////////////////////////////////////////

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

///////////////////////////////////////////////////////////////////////////////

class TMultiChunkWriterConfig
    : public TReplicationWriterConfig
    , public TErasureWriterConfig
{
public:
    i64 DesiredChunkSize;
    i64 MaxMetaSize;

    int UploadReplicationFactor;

    bool ChunksMovable;

    bool PreferLocalHost;

    bool SyncChunkSwitch;

    NErasure::ECodec ErasureCodec;

    TMultiChunkWriterConfig()
    {
        RegisterParameter("desired_chunk_size", DesiredChunkSize)
            .GreaterThan(0)
            .Default(1024 * 1024 * 1024);
        RegisterParameter("max_meta_size", MaxMetaSize)
            .GreaterThan(0)
            .LessThanOrEqual(64 * 1024 * 1024)
            .Default(30 * 1024 * 1024);
        RegisterParameter("upload_replication_factor", UploadReplicationFactor)
            .GreaterThanOrEqual(1)
            .Default(2);
        RegisterParameter("chunks_movable", ChunksMovable)
            .Default(true);
        RegisterParameter("prefer_local_host", PreferLocalHost)
            .Default(true);
        RegisterParameter("sync_chunk_switch", SyncChunkSwitch)
            .Default(true);
    }
};

///////////////////////////////////////////////////////////////////////////////

struct TMultiChunkWriterOptions
    : public virtual TEncodingWriterOptions
{
    int ReplicationFactor;
    Stroka Account;
    bool ChunksVital;

    NErasure::ECodec ErasureCodec;

    TMultiChunkWriterOptions()
    {
        RegisterParameter("replication_factor", ReplicationFactor)
            .GreaterThanOrEqual(1)
            .Default(3);
        RegisterParameter("account", Account)
            .NonEmpty();
        RegisterParameter("chunks_vital", ChunksVital)
            .Default(true);
        RegisterParameter("erasure_codec", ErasureCodec)
            .Default(NErasure::ECodec::None);
    }
};

///////////////////////////////////////////////////////////////////////////////

struct TMultiChunkReaderConfig
    : public virtual TReplicationReaderConfig
    , public virtual TSequentialReaderConfig
{
    i64 MaxBufferSize;

    TMultiChunkReaderConfig()
    {
        RegisterParameter("max_buffer_size", MaxBufferSize)
            .GreaterThan(0L)
            .LessThanOrEqual((i64) 10 * 1024 * 1024 * 1024)
            .Default((i64) 100 * 1024 * 1024);

        RegisterValidator([&] () {
            if (MaxBufferSize < 2 * WindowSize) {
                THROW_ERROR_EXCEPTION("\"max_buffer_size\" cannot be less than twice \"window_size\"");
            }
        });
    }
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
