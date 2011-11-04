#include "stdafx.h"
#include "table_manager.h"
#include "table_node_proxy.h"

namespace NYT {
namespace NTableServer {

using namespace NMetaState;
using namespace NCypress;
using namespace NChunkServer;
using namespace NTransaction;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TableServerLogger;

////////////////////////////////////////////////////////////////////////////////

TTableManager::TTableManager(
    TMetaStateManager* metaStateManager,
    TCompositeMetaState* metaState,
    TCypressManager* cypressManager,
    TChunkManager* chunkManager,
    TTransactionManager* transactionManager)
    : TMetaStatePart(metaStateManager, metaState)
    , CypressManager(cypressManager)
    , ChunkManager(chunkManager)
    , TransactionManager(transactionManager)
{
    YASSERT(cypressManager != NULL);
    YASSERT(chunkManager != NULL);
    YASSERT(transactionManager != NULL);

    RegisterMethod(this, &TThis::AddTableChunks);

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

void TTableManager::ValidateTransactionId(
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

TTableNode& TTableManager::GetTableNode(const TNodeId& nodeId, const TTransactionId& transactionId)
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

TChunk& TTableManager::GetChunk(const TChunkId& chunkId)
{
    auto* chunk = ChunkManager->FindChunkForUpdate(chunkId);
    if (chunk == NULL) {
        ythrow TServiceException(EErrorCode::NoSuchChunk) << 
            Sprintf("Invalid chunk id (ChunkId: %s)", ~chunkId.ToString());
    }
    return *chunk;
}

TMetaChange<TVoid>::TPtr
TTableManager::InitiateAddTableChunks(
    const TNodeId& nodeId,
    const TTransactionId& transactionId,
    const yvector<TChunkId>& chunkIds)
{
    TMsgAddTableChunks message;
    message.SetTransactionId(transactionId.ToProto());
    message.SetNodeId(nodeId.ToProto());
    ToProto<TChunkId, Stroka>(*message.MutableChunkIds(), chunkIds);

    return CreateMetaChange(
        MetaStateManager,
        message,
        &TThis::AddTableChunks,
        TPtr(this),
        ECommitMode::MayFail);
}

TVoid TTableManager::AddTableChunks(const NProto::TMsgAddTableChunks& message)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto transactionId = TTransactionId::FromProto(message.GetTransactionId());
    auto nodeId = TNodeId::FromProto(message.GetNodeId());
    //auto chunkId = TChunkId::FromProto(message.GetChunkId());

    //ValidateTransactionId(transactionId, false);

    //auto& chunk = GetChunk(chunkId);
    //auto& tableNode = GetTableNode(nodeId, transactionId);

    //if (tableNode.GetChunkListId() != NullChunkListId) {
    //    // TODO: exception type
    //    throw yexception() << "Chunk is already assigned to table node";
    //}

    //auto& chunkList = ChunkManager->CreateChunkList();
    //tableNode.SetChunkListId(chunkList.GetId());
    //ChunkManager->RefChunkList(chunkList);

    //chunkList.ChunkIds().push_back(chunkId);
    //ChunkManager->RefChunk(chunk);

    return TVoid();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT
