#pragma once

#include "common.h"
#include "file_node.h"
#include "file_manager.pb.h"
#include "file_service_rpc.h"

#include "../transaction_manager/transaction_manager.h"
#include "../chunk_server/chunk_manager.h"
#include "../chunk_server/chunk.h"
#include "../cypress/cypress_manager.h"
#include "../meta_state/meta_state_manager.h"
#include "../meta_state/composite_meta_state.h"
#include "../meta_state/meta_change.h"

namespace NYT {
namespace NFileServer {

////////////////////////////////////////////////////////////////////////////////
   
//! Manages files.
class TFileManager
    : public NMetaState::TMetaStatePart
{
public:
    typedef TIntrusivePtr<TFileManager> TPtr;

    //! Creates an instance.
    TFileManager(
        NMetaState::TMetaStateManager* metaStateManager,
        NMetaState::TCompositeMetaState* metaState,
        NCypress::TCypressManager* cypressManager,
        NChunkServer::TChunkManager* chunkManager,
        NTransaction::TTransactionManager* transactionManager);

    NMetaState::TMetaChange<TVoid>::TPtr InitiateSetFileChunk(
        const NCypress::TNodeId& nodeId,
        const NTransaction::TTransactionId& transactionId,
        const TChunkId& chunkId);

    TChunkId GetFileChunk(
        const NCypress::TNodeId& nodeId,
        const NTransaction::TTransactionId& transactionId);

private:
    typedef TFileServiceProxy::EErrorCode EErrorCode;
    typedef NRpc::TTypedServiceException<EErrorCode> TServiceException;
    typedef TFileManager TThis;

    NCypress::TCypressManager::TPtr CypressManager;
    NChunkServer::TChunkManager::TPtr ChunkManager;
    NTransaction::TTransactionManager::TPtr TransactionManager;

    void ValidateTransactionId(
        const NTransaction::TTransactionId& transactionId,
        bool mayBeNull);

    NChunkServer::TChunk& GetChunk(const TChunkId& chunkId);
    TFileNode& GetFileNode(
        const NCypress::TNodeId& nodeId,
        const NTransaction::TTransactionId& transactionId);

    TVoid SetFileChunk(const NProto::TMsgSetFileChunk& message);

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT
