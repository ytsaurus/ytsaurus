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
        NMetaState::TMetaStateManager* metaStateManager,
        TChunkManager* chunkManager,
        NTransaction::TTransactionManager* transactionManager,
        NRpc::IServer* server);

private:
    typedef TChunkService TThis;
    typedef TChunkServiceProxy::EErrorCode EErrorCode;
    typedef NRpc::TTypedServiceException<EErrorCode> TServiceException;

    TChunkManager::TPtr ChunkManager;
    TTransactionManager::TPtr TransactionManager;

    void ValidateHolderId(THolderId holderId);
    void ValidateTransactionId(const TTransactionId& transactionId);
    void ValidateChunkId(const TChunkId& chunkId);

    RPC_SERVICE_METHOD_DECL(NProto, RegisterHolder);
    RPC_SERVICE_METHOD_DECL(NProto, HolderHeartbeat);
    RPC_SERVICE_METHOD_DECL(NProto, CreateChunk);
    RPC_SERVICE_METHOD_DECL(NProto, FindChunk);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
