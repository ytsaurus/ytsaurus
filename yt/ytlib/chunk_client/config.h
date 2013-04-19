#pragma once

#include "public.h"

#include <ytlib/misc/error.h>

#include <ytlib/ytree/yson_serializable.h>

#include <ytlib/compression/codec.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

struct TRemoteReaderConfig
    : public virtual TYsonSerializable
{
    //! Timeout for a block request.
    TDuration NodeRpcTimeout;

    //! Time to wait before asking the master for seeds.
    TDuration RetryBackoffTime;

    //! Maximum number of attempts to fetch new seeds.
    int RetryCount;

    //! Time to wait before making another pass with same seeds.
    TDuration PassBackoffTime;

    //! Maximum number of passes with same seeds.
    int PassCount;

    //! Enable fetching blocks from peers suggested by seeds.
    bool FetchFromPeers;

    //! Publish ourselves as a peer capable of serving block requests.
    bool PublishPeer;

    //! Timeout after which a node forgets about the peer.
    TDuration PeerExpirationTimeout;

    //! Address to publish.
    Stroka PeerAddress;

    //! If True then fetched blocks are cached by the node.
    bool EnableNodeCaching;

    //! If True then the master may be asked for seeds.
    bool AllowFetchingSeedsFromMaster;

    TRemoteReaderConfig()
    {
        Register("node_rpc_timeout", NodeRpcTimeout)
            .Default(TDuration::Seconds(120));
        Register("retry_backoff_time", RetryBackoffTime)
            .Default(TDuration::Seconds(3));
        Register("retry_count", RetryCount)
            .Default(20);
        Register("pass_backoff_time", PassBackoffTime)
            .Default(TDuration::Seconds(3));
        Register("pass_count", PassCount)
            .Default(500);
        Register("fetch_from_peers", FetchFromPeers)
            .Default(true);
        Register("publish_peer", PublishPeer)
            .Default(false);
        Register("peer_expiration_timeout", PeerExpirationTimeout)
            .Default(TDuration::Seconds(300));
        Register("enable_node_caching", EnableNodeCaching)
            .Default(true);
        Register("allow_fetching_seeds_from_master", AllowFetchingSeedsFromMaster)
            .Default(true);
    }
};

///////////////////////////////////////////////////////////////////////////////

struct TClientBlockCacheConfig
    : public virtual TYsonSerializable
{
    //! The maximum number of bytes that block are allowed to occupy.
    //! Zero means that no blocks are cached.
    i64 MaxSize;

    TClientBlockCacheConfig()
    {
        Register("max_size", MaxSize)
            .Default(0)
            .GreaterThanOrEqual(0);
    }
};

///////////////////////////////////////////////////////////////////////////////

struct TSequentialReaderConfig
    : public virtual TYsonSerializable
{
    //! Prefetch window size (in bytes).
    i64 WindowSize;

    //! Maximum amount of data to be transfered via a single RPC request.
    i64 GroupSize;

    TSequentialReaderConfig()
    {
        Register("window_size", WindowSize)
            .Default(64 * 1024 * 1024)
            .GreaterThan(0);
        Register("group_size", GroupSize)
            .Default(8 * 1024 * 1024)
            .GreaterThan(0);

        RegisterValidator([&] () {
            if (GroupSize > WindowSize) {
                THROW_ERROR_EXCEPTION("\"group_size\" cannot be larger than \"window_size\"");
            }
        });
    }
};

///////////////////////////////////////////////////////////////////////////////

struct TRemoteWriterConfig
    : public virtual TYsonSerializable
{
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

    //! Maximum allowed period of time without RPC requests to nodes.
    /*!
     *  If the writer remains inactive for the given period, it sends #TChunkHolderProxy::PingSession.
     */
    TDuration NodePingInterval;

    //! If True then written blocks are cached by the node.
    bool EnableNodeCaching;

    TRemoteWriterConfig()
    {
        Register("send_window_size", SendWindowSize)
            .Default(4 * 1024 * 1024)
            .GreaterThan(0);
        Register("group_size", GroupSize)
            .Default(1024 * 1024)
            .GreaterThan(0);
        Register("node_rpc_timeout", NodeRpcTimeout)
            .Default(TDuration::Seconds(120));
        Register("node_ping_interval", NodePingInterval)
            .Default(TDuration::Seconds(10));
        Register("enable_node_caching", EnableNodeCaching)
            .Default(false);

        RegisterValidator([&] () {
            if (SendWindowSize < GroupSize) {
                THROW_ERROR_EXCEPTION("\"window_size\" cannot be less than \"group_size\"");
            }
        });
    }
};

///////////////////////////////////////////////////////////////////////////////

struct TEncodingWriterConfig
    : public virtual TYsonSerializable
{
    i64 EncodeWindowSize;
    double DefaultCompressionRatio;
    bool VerifyCompression;

    TEncodingWriterConfig()
    {
        Register("encode_window_size", EncodeWindowSize)
            .Default(4 * 1024 * 1024)
            .GreaterThan(0);
        Register("default_compression_ratio", DefaultCompressionRatio)
            .Default(0.2);
        Register("verify_compression", VerifyCompression)
            .Default(true);
    }
};

///////////////////////////////////////////////////////////////////////////////

struct TEncodingWriterOptions
    : public virtual TYsonSerializable
{
    NCompression::ECodec Codec;

    TEncodingWriterOptions()
    {
        Register("compression_codec", Codec)
            .Default(NCompression::ECodec::None);
    }
};

///////////////////////////////////////////////////////////////////////////////

struct TDispatcherConfig
    : public virtual TYsonSerializable
{
    int CompressionPoolSize;

    TDispatcherConfig()
    {
        Register("compression_pool_size", CompressionPoolSize)
            .Default(4)
            .GreaterThan(0);
    }
};

///////////////////////////////////////////////////////////////////////////////

struct TMultiChunkWriterConfig
    : public TRemoteWriterConfig
{
    i64 DesiredChunkSize;
    i64 MaxMetaSize;

    int UploadReplicationFactor;

    bool ChunksMovable;
    bool ChunksVital;

    bool PreferLocalHost;

    TMultiChunkWriterConfig()
    {
        Register("desired_chunk_size", DesiredChunkSize)
            .GreaterThan(0)
            .Default(1024 * 1024 * 1024);
        Register("max_meta_size", MaxMetaSize)
            .GreaterThan(0)
            .LessThanOrEqual(64 * 1024 * 1024)
            .Default(30 * 1024 * 1024);
        Register("upload_replication_factor", UploadReplicationFactor)
            .GreaterThanOrEqual(1)
            .Default(2);
        Register("chunks_movable", ChunksMovable)
            .Default(true);
        Register("chunks_vital", ChunksVital)
            .Default(true);
        Register("prefer_local_host", PreferLocalHost)
            .Default(true);
    }
};

///////////////////////////////////////////////////////////////////////////////

struct TMultiChunkWriterOptions
    : public virtual TEncodingWriterOptions
{
    int ReplicationFactor;
    Stroka Account;

    TMultiChunkWriterOptions()
    {
        Register("replication_factor", ReplicationFactor)
            .GreaterThanOrEqual(1)
            .Default(3);
        Register("account", Account)
            .NonEmpty();
    }
};

///////////////////////////////////////////////////////////////////////////////

struct TMultiChunkReaderConfig
    : public virtual TRemoteReaderConfig
    , public virtual TSequentialReaderConfig
{
    i64 MaxBufferSize;

    TMultiChunkReaderConfig()
    {
        Register("max_buffer_size", MaxBufferSize)
            .GreaterThan(0L)
            .LessThanOrEqual(10L * 1024 * 1024 * 1024)
            .Default(256L * 1024 * 1024);

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
