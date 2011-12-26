#pragma once

#include "common.h"
#include "async_reader.h"

#include "../misc/configurable.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

struct TRemoteReaderConfig
    : TConfigurable
{
    typedef TIntrusivePtr<TRemoteReaderConfig> TPtr;

    //! Holder RPC requests timeout.
    TDuration HolderRpcTimeout;

    TRemoteReaderConfig()
    {
        Register("holder_rpc_timeout", HolderRpcTimeout).Default(TDuration::Seconds(30));
    }
};

///////////////////////////////////////////////////////////////////////////////

struct IRemoteReaderFactory
    : virtual TRefCountedBase
{
    typedef TIntrusivePtr<IRemoteReaderFactory> TPtr;

    virtual IAsyncReader::TPtr Create(
        const TChunkId& chunkId,
        const yvector<Stroka>& holderAddresses) = 0;

};

IRemoteReaderFactory::TPtr CreateRemoteReaderFactory(TRemoteReaderConfig* config);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
