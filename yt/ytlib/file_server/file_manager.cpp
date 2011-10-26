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

TFileManager::TFileManager(
    NMetaState::TMetaStateManager::TPtr metaStateManager,
    NMetaState::TCompositeMetaState::TPtr metaState,
    TCypressManager::TPtr cypressManager)
    : TMetaStatePart(metaStateManager, metaState)
    , CypressManager(cypressManager)
{
    YASSERT(~cypressManager != NULL);

    RegisterMethod(this, &TThis::SetFileChunk);

    metaState->RegisterPart(this);
}

TMetaChange<TVoid>::TPtr
TFileManager::InitiateSetFileChunk(
    const TTransactionId& transactionId,
    const TNodeId& nodeId,
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
        TPtr(this));
}

TVoid TFileManager::SetFileChunk(const NProto::TMsgSetFileChunk& message)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto transactionId = TTransactionId::FromProto(message.GetTransactionId());
    auto nodeId = TNodeId::FromProto(message.GetNodeId());
    auto chunkId = TChunkId::FromProto(message.GetChunkId());

    auto node = CypressManager->GetNode(nodeId, transactionId);
    auto* typedNode = dynamic_cast<TFileNodeProxy*>(~node);
    YASSERT(typedNode != NULL);

    typedNode->SetChunkId(chunkId);

    return TVoid();
}

Stroka TFileManager::GetPartName() const
{
    return "FileManager";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT
