#pragma once

#include "public.h"
#include "chunk_holder_service_proxy.h"

#include <ytlib/rpc/service.h>
#include <ytlib/rpc/channel_cache.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

class TChunkHolderService
    : public NRpc::TServiceBase
{
public:
    //! Creates an instance.
    TChunkHolderService(TChunkHolderConfigPtr config, TBootstrap* bootstrap);

private:
    typedef TChunkHolderService TThis;
    typedef TChunkHolderServiceProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;

    TChunkHolderConfigPtr Config;
    TBootstrap* Bootstrap;
    NRpc::TChannelCache ChannelCache;

    DECLARE_RPC_SERVICE_METHOD(NProto, StartChunk);
    DECLARE_RPC_SERVICE_METHOD(NProto, FinishChunk);
    DECLARE_RPC_SERVICE_METHOD(NProto, PutBlocks);
    DECLARE_RPC_SERVICE_METHOD(NProto, SendBlocks);
    DECLARE_RPC_SERVICE_METHOD(NProto, FlushBlock);
    DECLARE_RPC_SERVICE_METHOD(NProto, GetBlocks);
    DECLARE_RPC_SERVICE_METHOD(NProto, PingSession);
    DECLARE_RPC_SERVICE_METHOD(NProto, GetChunkMeta);
    DECLARE_RPC_SERVICE_METHOD(NProto, PrecacheChunk);
    DECLARE_ONE_WAY_RPC_SERVICE_METHOD(NProto, UpdatePeer);
    DECLARE_RPC_SERVICE_METHOD(NProto, GetTableSamples);

    void ValidateNoSession(const TChunkId& chunkId);
    void ValidateNoChunk(const TChunkId& chunkId);

    TIntrusivePtr<TSession> GetSession(const TChunkId& chunkId);
    TIntrusivePtr<TChunk> GetChunk(const TChunkId& chunkId);

    bool CheckThrottling() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
