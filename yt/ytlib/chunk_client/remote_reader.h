#pragma once

#include "common.h"
#include "async_reader.h"
#include "block_cache.h"

#include <ytlib/misc/configurable.h>
#include <ytlib/rpc/public.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

struct TRemoteReaderConfig
    : public TConfigurable
{
    typedef TIntrusivePtr<TRemoteReaderConfig> TPtr;

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

IAsyncReader::TPtr CreateRemoteReader(
    TRemoteReaderConfig* config,
    IBlockCache* blockCache,
    NRpc::IChannel* masterChannel,
    const TChunkId& chunkId,
    const yvector<Stroka>& seedAddresses = yvector<Stroka>());

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
