#pragma once

#include "public.h"
#include "chunk.h"

#include <ytlib/rpc/service.h>

#include <ytlib/actions/action_queue.h>

#include <ytlib/chunk_client/data_node_service_proxy.h>

#include <ytlib/table_client/public.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

class TDataNodeService
    : public NRpc::TServiceBase
{
public:
    TDataNodeService(
        TDataNodeConfigPtr config,
        TBootstrap* bootstrap);

private:
    typedef TDataNodeService TThis;
    typedef NChunkClient::TDataNodeServiceProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;

    TDataNodeConfigPtr Config;
    TActionQueuePtr WorkerThread;
    TBootstrap* Bootstrap;

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, StartChunk);
    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, FinishChunk);
    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, PutBlocks);
    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, SendBlocks);
    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, FlushBlock);
    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetBlocks);
    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, PingSession);
    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetChunkMeta);
    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, PrecacheChunk);
    DECLARE_ONE_WAY_RPC_SERVICE_METHOD(NChunkClient::NProto, UpdatePeer);
    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetTableSamples);
    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetChunkSplits);

    void ValidateNoSession(const TChunkId& chunkId);
    void ValidateNoChunk(const TChunkId& chunkId);

    TIntrusivePtr<TSession> GetSession(const TChunkId& chunkId);
    TIntrusivePtr<TChunk> GetChunk(const TChunkId& chunkId);
    void ProcessSample(
        const NChunkClient::NProto::TReqGetTableSamples::TSampleRequest* sampleRequest,
        NChunkClient::NProto::TRspGetTableSamples::TChunkSamples* chunkSamples,
        const NTableClient::TKeyColumns& keyColumns,
        TChunk::TGetMetaResult result);

    void MakeChunkSplits(
        const NTableClient::NProto::TInputChunk* inputChunk,
        NChunkClient::NProto::TRspGetChunkSplits::TChunkSplits* splittedChunk,
        i64 minSplitSize,
        const NTableClient::TKeyColumns& keyColumns,
        TChunk::TGetMetaResult result);

    void OnGotChunkMeta(TCtxGetChunkMetaPtr context, TNullable<int> artitionTag, TChunk::TGetMetaResult result);

    bool CheckThrottling() const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
