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

using NMetaState::TMetaChange;
using NMetaState::TMetaStateManager;
using NMetaState::TCompositeMetaState;
using NCypress::TCypressManager;
using NChunkServer::TChunkManager;
using NTransaction::TTransactionManager;

////////////////////////////////////////////////////////////////////////////////
   
// TODO: possibly merge into TFileManager
class TFileManagerBase
{
protected:
    typedef TFileServiceProxy::EErrorCode EErrorCode;
    typedef NRpc::TTypedServiceException<EErrorCode> TServiceException;

    TCypressManager::TPtr CypressManager;
    TChunkManager::TPtr ChunkManager;
    TTransactionManager::TPtr TransactionManager;

    TFileManagerBase(
        TCypressManager::TPtr cypressManager,
        TChunkManager::TPtr chunkManager,
        TTransactionManager::TPtr transactionManager);

    void ValidateTransactionId(const TTransactionId& transactionId);
    void ValidateChunkId(const TChunkId& chunkId);
    TFileNode& GetFileNode(const TNodeId& nodeId, const TTransactionId& transactionId);

};

////////////////////////////////////////////////////////////////////////////////

//! Manages files.
class TFileManager
    : public NMetaState::TMetaStatePart
    , public TFileManagerBase
{
public:
    typedef TIntrusivePtr<TFileManager> TPtr;

    //! Creates an instance.
    TFileManager(
        TMetaStateManager::TPtr metaStateManager,
        TCompositeMetaState::TPtr metaState,
        TCypressManager::TPtr cypressManager,
        TChunkManager::TPtr chunkManager,
        TTransactionManager::TPtr transactionManager);

    TMetaChange<TVoid>::TPtr InitiateSetFileChunk(
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        const TChunkId& chunkId);

    TChunkId GetFileChunk(
        const TNodeId& nodeId,
        const TTransactionId& transactionId);

private:
    typedef TFileManager TThis;

    virtual Stroka GetPartName() const;

    TVoid SetFileChunk(const NProto::TMsgSetFileChunk& message);

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT
