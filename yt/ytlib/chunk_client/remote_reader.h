#pragma once

#include "common.h"
#include "async_reader.h"
#include "block_cache.h"

#include "../misc/configurable.h"
#include "../rpc/channel.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

struct TRemoteReaderConfig
    : TConfigurable
{
    typedef TIntrusivePtr<TRemoteReaderConfig> TPtr;

    //! Timeout for a block request.
    TDuration HolderRpcTimeout;
    
    //! Timeout for seeds request.
    TDuration MasterRpcTimeout;
    
    //! A time to wait before asking the master for seeds.
    TDuration RetryBackoffTime;
    
    //! Maximum number of attempts to fetch new seeds.
    int RetryCount;

    //! A time to wait before making another pass with same seeds.
    TDuration PassBackoffTime;

    //! Maximum number of passes with same seeds.
    int PassCount;

    //! Enable fetching blocks from peers suggested by seeds.
    bool EnablePeering;

    TRemoteReaderConfig()
    {
        Register("holder_rpc_timeout", HolderRpcTimeout).Default(TDuration::Seconds(30));
        Register("master_rpc_timeout", MasterRpcTimeout).Default(TDuration::Seconds(5));
        Register("retry_backoff_time", RetryBackoffTime).Default(TDuration::Seconds(1));
        Register("retry_count", RetryCount).Default(100);
        Register("pass_backoff_time", PassBackoffTime).Default(TDuration::MilliSeconds(100));
        Register("pass_count", PassCount).Default(3);
        Register("enable_peering", EnablePeering).Default(true);
    }
};

IAsyncReader::TPtr CreateRemoteReader(
    TRemoteReaderConfig* config,
    IBlockCache* blockCache,
    NRpc::IChannel* masterChannel,
    const TChunkId& chunkId,
    const yvector<Stroka>& seedAddresses = yvector<Stroka>(),
    const TPeerInfo& peer = TPeerInfo());

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
