#pragma once

#include "common.h"
#include "chunk_manager.h"
#include "chunk_service_proxy.h"

#include <ytlib/meta_state/meta_state_service.h>
#include <ytlib/rpc/server.h>

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
        NTransactionServer::TTransactionManager* transactionManager);

private:
    typedef TChunkService TThis;
    typedef TChunkServiceProxy::EErrorCode EErrorCode;

    TChunkManager::TPtr ChunkManager;
    NTransactionServer::TTransactionManager::TPtr TransactionManager;

    void ValidateHolderId(THolderId holderId);
    void ValidateTransactionId(const TTransactionId& transactionId);
    void ValidateChunkId(const TChunkId& chunkId);
    void ValidateChunkListId(const TChunkListId& chunkListId);
    void ValidateChunkTreeId(const TChunkTreeId& chunkTreeId);

    DECLARE_RPC_SERVICE_METHOD(NProto, RegisterHolder);
    DECLARE_RPC_SERVICE_METHOD(NProto, HolderHeartbeat);
    DECLARE_RPC_SERVICE_METHOD(NProto, CreateChunks);
    DECLARE_RPC_SERVICE_METHOD(NProto, ConfirmChunks);
    DECLARE_RPC_SERVICE_METHOD(NProto, CreateChunkLists);
    DECLARE_RPC_SERVICE_METHOD(NProto, AttachChunkTrees);
    DECLARE_RPC_SERVICE_METHOD(NProto, DetachChunkTrees);
    DECLARE_RPC_SERVICE_METHOD(NProto, LocateChunk);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
