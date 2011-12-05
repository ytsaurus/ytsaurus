#pragma once

#include "common.h"
#include "chunk_manager.h"
#include "chunk_service_rpc.h"

#include "../meta_state/meta_state_service.h"
#include "../rpc/server.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkService
    : public NMetaState::TMetaStateServiceBase
{
public:
    typedef TIntrusivePtr<TChunkService> TPtr;

    //! Creates an instance.
    TChunkService(
        NMetaState::IMetaStateManager* metaStateManager,
        TChunkManager* chunkManager,
        NTransactionServer::TTransactionManager* transactionManager,
        NRpc::IRpcServer* server);

private:
    typedef TChunkService TThis;
    typedef TChunkServiceProxy::EErrorCode EErrorCode;

    TChunkManager::TPtr ChunkManager;
    TTransactionManager::TPtr TransactionManager;

    void ValidateHolderId(THolderId holderId);
    void ValidateTransactionId(const TTransactionId& transactionId);
    void ValidateChunkId(const NChunkClient::TChunkId& chunkId);

    DECLARE_RPC_SERVICE_METHOD(NProto, RegisterHolder);
    DECLARE_RPC_SERVICE_METHOD(NProto, HolderHeartbeat);
    DECLARE_RPC_SERVICE_METHOD(NProto, CreateChunk);
    DECLARE_RPC_SERVICE_METHOD(NProto, FindChunk);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
