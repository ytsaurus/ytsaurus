#include "stdafx.h"
#include "table_manager.h"
#include "table_node_proxy.h"

namespace NYT {
namespace NTableServer {

using namespace NMetaState;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TableServerLogger;

////////////////////////////////////////////////////////////////////////////////

TTableManagerBase::TTableManagerBase(
    TCypressManager* cypressManager,
    TChunkManager* chunkManager,
    TTransactionManager* transactionManager)
    : CypressManager(cypressManager)
    , ChunkManager(chunkManager)
    , TransactionManager(transactionManager)
{
    YASSERT(cypressManager != NULL);
    YASSERT(chunkManager != NULL);
    YASSERT(transactionManager != NULL);
}

void TTableManagerBase::ValidateTransactionId(
    const TTransactionId& transactionId,
    bool mayBeNull)
{
    if ((transactionId != NullTransactionId || !mayBeNull) &&
        TransactionManager->FindTransaction(transactionId) == NULL)
    {
        ythrow TServiceException(EErrorCode::NoSuchTransaction) << 
            Sprintf("Invalid transaction id (TransactionId: %s)", ~transactionId.ToString());
    }
}

TTableNode& TTableManagerBase::GetTableNode(const TNodeId& nodeId, const TTransactionId& transactionId)
{
    auto* impl = CypressManager->FindTransactionNodeForUpdate(nodeId, transactionId);
    if (impl == NULL) {
        ythrow TServiceException(EErrorCode::NoSuchNode) << 
            Sprintf("Invalid table node id (NodeId: %s, TransactionId: %s)",
                ~nodeId.ToString(),
                ~transactionId.ToString());
    }

    auto* typedImpl = dynamic_cast<TTableNode*>(impl);
    if (typedImpl == NULL) {
        ythrow TServiceException(EErrorCode::NotATable) << 
            Sprintf("Not a table node (NodeId: %s, TransactionId: %s)",
                ~nodeId.ToString(),
                ~transactionId.ToString());
    }

    return *typedImpl;
}

TChunk& TTableManagerBase::GetChunk(const TChunkId& chunkId)
{
    auto* chunk = ChunkManager->FindChunkForUpdate(chunkId);
    if (chunk == NULL) {
        ythrow TServiceException(EErrorCode::NoSuchChunk) << 
            Sprintf("Invalid chunk id (ChunkId: %s)", ~chunkId.ToString());
    }
    return *chunk;
}

////////////////////////////////////////////////////////////////////////////////

TTableManager::TTableManager(
    TMetaStateManager* metaStateManager,
    TCompositeMetaState* metaState,
    TCypressManager* cypressManager,
    TChunkManager* chunkManager,
    TTransactionManager* transactionManager)
    : TMetaStatePart(metaStateManager, metaState)
    , TTableManagerBase(cypressManager, chunkManager, transactionManager)
{
    YASSERT(cypressManager != NULL);

    RegisterMethod(this, &TThis::SetTableChunk);

    cypressManager->RegisterNodeType(~New<TTableNodeTypeHandler>(
        cypressManager,
        this,
        chunkManager));

    metaState->RegisterPart(this);
}

Stroka TTableManager::GetPartName() const
{
    return "TableManager";
}

TMetaChange<TVoid>::TPtr
TTableManager::InitiateSetTableChunk(
    const TNodeId& nodeId,
    const TTransactionId& transactionId,
    const TChunkId& chunkId)
{
    TMsgSetTableChunk message;
    message.SetTransactionId(transactionId.ToProto());
    message.SetNodeId(nodeId.ToProto());
    message.SetChunkId(chunkId.ToProto());

    return CreateMetaChange(
        MetaStateManager,
        message,
        &TThis::SetTableChunk,
        TPtr(this),
        ECommitMode::MayFail);
}

TVoid TTableManager::SetTableChunk(const NProto::TMsgSetTableChunk& message)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto transactionId = TTransactionId::FromProto(message.GetTransactionId());
    auto nodeId = TNodeId::FromProto(message.GetNodeId());
    auto chunkId = TChunkId::FromProto(message.GetChunkId());

    ValidateTransactionId(transactionId, false);

    auto& chunk = GetChunk(chunkId);
    auto& tableNode = GetTableNode(nodeId, transactionId);

    if (tableNode.GetChunkListId() != NullChunkListId) {
        // TODO: exception type
        throw yexception() << "Chunk is already assigned to table node";
    }

    auto& chunkList = ChunkManager->CreateChunkList();
    tableNode.SetChunkListId(chunkList.GetId());
    ChunkManager->RefChunkList(chunkList);

    chunkList.Chunks().push_back(chunkId);
    ChunkManager->RefChunk(chunk);

    return TVoid();
}

TChunkId TTableManager::GetTableChunk(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
    ValidateTransactionId(transactionId, true);
    auto& tableNode = GetTableNode(nodeId, transactionId);

    if (tableNode.GetChunkListId() == NullChunkId) {
        return NullChunkId;
    }

    const auto& chunkList = ChunkManager->GetChunkList(tableNode.GetChunkListId());
    YASSERT(chunkList.Chunks().ysize() == 1);
    return chunkList.Chunks()[0];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT
