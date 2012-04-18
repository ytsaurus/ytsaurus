#pragma once

#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

struct TRemoteReaderConfig
    : public TConfigurable
{
    //! Timeout for a block request.
    TDuration HolderRpcTimeout;

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

    //! Timeout after which a holder forgets about the peer.
    TDuration PeerExpirationTimeout;

    //! Address to publish.
    Stroka PeerAddress;

    TRemoteReaderConfig()
    {
        Register("holder_rpc_timeout", HolderRpcTimeout).Default(TDuration::Seconds(30));
        Register("retry_backoff_time", RetryBackoffTime).Default(TDuration::Seconds(3));
        Register("retry_count", RetryCount).Default(100);
        Register("pass_backoff_time", PassBackoffTime).Default(TDuration::Seconds(1));
        Register("pass_count", PassCount).Default(3);
        Register("fetch_from_peers", FetchFromPeers).Default(true);
        Register("publish_peer", PublishPeer).Default(false);
        Register("peer_expiration_timeout", PeerExpirationTimeout).Default(TDuration::Seconds(300));
    }
};

///////////////////////////////////////////////////////////////////////////////

struct TClientBlockCacheConfig
    : public TConfigurable
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
    : public TConfigurable
{
    //! Prefetch window size (in bytes).
    i64 WindowSize;

    //! Maximum amount of data to be transfered via a single RPC request.
    i64 GroupSize;

    TSequentialReaderConfig()
    {
        Register("window_size", WindowSize)
            .Default(16 * 1024 * 1024)
            .GreaterThan(0);
        Register("group_size", GroupSize)
            .Default(8 * 1024 * 1024)
            .GreaterThan(0);
    }

    virtual void DoValidate() const
    {
        if (GroupSize > WindowSize) {
            ythrow yexception() << "\"group_size\" cannot be larger than \"prefetch_window_size\"";
        }
    }
};

///////////////////////////////////////////////////////////////////////////////

struct TRemoteWriterConfig
    : public TConfigurable
{
    //! Maximum window size (in bytes).
    int WindowSize;
        
    //! Maximum group size (in bytes).
    int GroupSize;
        
    //! RPC requests timeout.
    /*!
        *  This timeout is especially useful for PutBlocks calls to ensure that
        *  uploading is not stalled.
        */
    TDuration HolderRpcTimeout;

    //! Maximum allowed period of time without RPC requests to holders.
    /*!
        *  If the writer remains inactive for the given period, it sends #TChunkHolderProxy::PingSession.
        */
    TDuration SessionPingInterval;

    TRemoteWriterConfig()
    {
        Register("window_size", WindowSize)
            .Default(4 * 1024 * 1024)
            .GreaterThan(0);
        Register("group_size", GroupSize)
            .Default(1024 * 1024)
            .GreaterThan(0);
        Register("holder_rpc_timeout", HolderRpcTimeout)
            .Default(TDuration::Seconds(30));
        Register("session_ping_interval", SessionPingInterval)
            .Default(TDuration::Seconds(10));
    }

    virtual void DoValidate() const
    {
        if (WindowSize < GroupSize) {
            ythrow yexception() << "\"window_size\" cannot be less than \"group_size\"";
        }
    }
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
