#include "stdafx.h"
#include "file_manager.h"
#include "file_node_proxy.h"

namespace NYT {
namespace NFileServer {

using namespace NMetaState;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = FileServerLogger;

////////////////////////////////////////////////////////////////////////////////

TFileManagerBase::TFileManagerBase(
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

void TFileManagerBase::ValidateTransactionId(const TTransactionId& transactionId)
{
    if (transactionId != NullTransactionId &&
        TransactionManager->FindTransaction(transactionId) == NULL)
    {
        ythrow TServiceException(EErrorCode::NoSuchTransaction) << 
            Sprintf("Invalid transaction id (TransactionId: %s)", ~transactionId.ToString());
    }
}

void TFileManagerBase::ValidateChunkId(const TChunkId& chunkId)
{
    if (ChunkManager->FindChunk(chunkId) == NULL) {
        ythrow TServiceException(EErrorCode::NoSuchChunk) << 
            Sprintf("Invalid chunk id (ChunkId: %s)", ~chunkId.ToString());
    }
}

TFileNode& TFileManagerBase::GetFileNode(const TNodeId& nodeId, const TTransactionId& transactionId)
{
    auto* impl = CypressManager->FindTransactionNodeForUpdate(nodeId, transactionId);
    if (impl == NULL) {
        ythrow TServiceException(EErrorCode::NoSuchNode) << 
            Sprintf("Invalid file node id (NodeId: %s, TransactionId: %s)",
                ~nodeId.ToString(),
                ~transactionId.ToString());
    }

    auto* typedImpl = dynamic_cast<TFileNode*>(impl);
    if (typedImpl == NULL) {
        ythrow TServiceException(EErrorCode::NotAFile) << 
            Sprintf("Not a file node (NodeId: %s, TransactionId: %s)",
                ~nodeId.ToString(),
                ~transactionId.ToString());
    }

    return *typedImpl;
}

////////////////////////////////////////////////////////////////////////////////

TFileManager::TFileManager(
    TMetaStateManager* metaStateManager,
    TCompositeMetaState* metaState,
    TCypressManager* cypressManager,
    TChunkManager* chunkManager,
    TTransactionManager* transactionManager)
    : TMetaStatePart(metaStateManager, metaState)
    , TFileManagerBase(cypressManager, chunkManager, transactionManager)
{
    YASSERT(cypressManager != NULL);

    RegisterMethod(this, &TThis::SetFileChunk);

    cypressManager->RegisterNodeType(~New<TFileNodeTypeHandler>(
        cypressManager,
        this,
        chunkManager));

    metaState->RegisterPart(this);
}

Stroka TFileManager::GetPartName() const
{
    return "FileManager";
}

TMetaChange<TVoid>::TPtr
TFileManager::InitiateSetFileChunk(
    const TNodeId& nodeId,
    const TTransactionId& transactionId,
    const TChunkId& chunkId)
{
    TMsgSetFileChunk message;
    message.SetTransactionId(transactionId.ToProto());
    message.SetNodeId(nodeId.ToProto());
    message.SetChunkId(chunkId.ToProto());

    return CreateMetaChange(
        MetaStateManager,
        message,
        &TThis::SetFileChunk,
        TPtr(this),
        ECommitMode::MayFail);
}

TVoid TFileManager::SetFileChunk(const NProto::TMsgSetFileChunk& message)
{
    //VERIFY_THREAD_AFFINITY(StateThread);

    //auto transactionId = TTransactionId::FromProto(message.GetTransactionId());
    //auto nodeId = TNodeId::FromProto(message.GetNodeId());
    //auto chunkId = TChunkId::FromProto(message.GetChunkId());

    //ValidateTransactionId(transactionId);
    //ValidateChunkId(chunkId);

    //auto& impl = GetFileNode(nodeId, transactionId);
    //impl.SetChunkId(chunkId);

    return TVoid();
}

TChunkId TFileManager::GetFileChunk(
    const TNodeId& nodeId,
    const TTransactionId& transactionId)
{
    //ValidateTransactionId(transactionId);
    //auto& impl = GetFileNode(nodeId, transactionId);
    //return impl.GetChunkId();
    // 
    return NullChunkId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT
