#pragma once

#include "common.h"
#include "table_node.h"
#include "table_manager.pb.h"
#include "table_service_rpc.h"

#include "../transaction_manager/transaction_manager.h"
#include "../chunk_server/chunk_manager.h"
#include "../chunk_server/chunk.h"
#include "../cypress/cypress_manager.h"
#include "../meta_state/meta_state_manager.h"
#include "../meta_state/composite_meta_state.h"
#include "../meta_state/meta_change.h"

namespace NYT {
namespace NTableServer {

using NMetaState::TMetaChange;
using NMetaState::TMetaStateManager;
using NMetaState::TCompositeMetaState;
using NCypress::TCypressManager;
using NChunkServer::TChunkManager;
using NChunkServer::TChunk;
using NTransaction::TTransactionManager;

////////////////////////////////////////////////////////////////////////////////
   
// TODO: possibly merge into TTableManager
class TTableManagerBase
{
protected:
    typedef TTableServiceProxy::EErrorCode EErrorCode;
    typedef NRpc::TTypedServiceException<EErrorCode> TServiceException;

    TCypressManager::TPtr CypressManager;
    TChunkManager::TPtr ChunkManager;
    TTransactionManager::TPtr TransactionManager;

    TTableManagerBase(
        TCypressManager* cypressManager,
        TChunkManager* chunkManager,
        TTransactionManager* transactionManager);

    void ValidateTransactionId(const TTransactionId& transactionId, bool mayBeNull);

    TChunk& GetChunk(const TChunkId& chunkId);
    TTableNode& GetTableNode(const TNodeId& nodeId, const TTransactionId& transactionId);

};

////////////////////////////////////////////////////////////////////////////////

//! Manages tables.
class TTableManager
    : public NMetaState::TMetaStatePart
    , public TTableManagerBase
{
public:
    typedef TIntrusivePtr<TTableManager> TPtr;

    //! Creates an instance.
    TTableManager(
        TMetaStateManager* metaStateManager,
        TCompositeMetaState* metaState,
        TCypressManager* cypressManager,
        TChunkManager* chunkManager,
        TTransactionManager* transactionManager);

    TMetaChange<TVoid>::TPtr InitiateSetTableChunk(
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        const TChunkId& chunkId);

    TChunkId GetTableChunk(
        const TNodeId& nodeId,
        const TTransactionId& transactionId);

private:
    typedef TTableManager TThis;

    virtual Stroka GetPartName() const;

    TVoid SetTableChunk(const NProto::TMsgSetTableChunk& message);

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT
