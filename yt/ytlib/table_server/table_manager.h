#pragma once

#include "common.h"
#include "table_node.h"
#include "table_manager.pb.h"
#include "table_service_rpc.h"

#include "../transaction_server/transaction_manager.h"
#include "../chunk_server/chunk_manager.h"
#include "../chunk_server/chunk.h"
#include "../cypress/cypress_manager.h"
#include "../meta_state/meta_state_manager.h"
#include "../meta_state/composite_meta_state.h"
#include "../meta_state/meta_change.h"

namespace NYT {
namespace NTableServer {

////////////////////////////////////////////////////////////////////////////////
   
//! Manages tables.
class TTableManager
    : public NMetaState::TMetaStatePart
{
public:
    typedef TIntrusivePtr<TTableManager> TPtr;

    //! Creates an instance.
    TTableManager(
        NMetaState::TMetaStateManager* metaStateManager,
        NMetaState::TCompositeMetaState* metaState,
        NCypress::TCypressManager* cypressManager,
        NChunkServer::TChunkManager* chunkManager,
        NTransaction::TTransactionManager* transactionManager);

    NMetaState::TMetaChange<TVoid>::TPtr InitiateAddTableChunks(
        const NCypress::TNodeId& nodeId,
        const NTransaction::TTransactionId& transactionId,
        const yvector<TChunkId>& chunkIds);

private:
    typedef TTableServiceProxy::EErrorCode EErrorCode;
    typedef NRpc::TTypedServiceException<EErrorCode> TServiceException;
    typedef TTableManager TThis;

    NCypress::TCypressManager::TPtr CypressManager;
    NChunkServer::TChunkManager::TPtr ChunkManager;
    NTransaction::TTransactionManager::TPtr TransactionManager;

    void ValidateTransactionId(
        const NTransaction::TTransactionId& transactionId,
        bool mayBeNull);

    NChunkServer::TChunk& GetChunk(const TChunkId& chunkId);
    
    TTableNode& GetTableNode(
        const NCypress::TNodeId& nodeId,
        const NTransaction::TTransactionId& transactionId);

    TVoid AddTableChunks(const NProto::TMsgAddTableChunks& message);

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT
