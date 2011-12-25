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
    TDuration BackoffTime;
    //! Maximum number of attempts.
    int RetryCount;

    TRemoteReaderConfig()
    {
        Register("holder_rpc_timeout", HolderRpcTimeout).Default(TDuration::Seconds(30));
        Register("master_rpc_timeout", MasterRpcTimeout).Default(TDuration::Seconds(5));
        Register("backoff_time", BackoffTime).Default(TDuration::Seconds(1));
        Register("retry_count", RetryCount).Default(300);
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
