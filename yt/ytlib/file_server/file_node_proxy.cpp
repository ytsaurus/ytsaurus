#include "stdafx.h"
#include "file_node_proxy.h"
#include "file_chunk_meta.pb.h"

#include "../misc/string.h"
#include <yt/ytlib/object_server/id.h>

namespace NYT {
namespace NFileServer {

using namespace NChunkServer;
using namespace NCypress;
using namespace NYTree;
using namespace NRpc;
using namespace NFileClient::NProto;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

TFileNodeProxy::TFileNodeProxy(
    INodeTypeHandler* typeHandler,
    TCypressManager* cypressManager,
    TChunkManager* chunkManager,
    const TTransactionId& transactionId,
    const TNodeId& nodeId)
    : TCypressNodeProxyBase<IEntityNode, TFileNode>(
        typeHandler,
        cypressManager,
        transactionId,
        nodeId)
    , ChunkManager(chunkManager)
{ }

void TFileNodeProxy::DoInvoke(IServiceContext* context)
{
    Stroka verb = context->GetVerb();
    if (verb == "Fetch") {
        FetchThunk(context);
    } else {
        TBase::DoInvoke(context);
    }
}

bool TFileNodeProxy::IsExecutable()
{
    // TODO(babenko): fetch this attribute
    return false;
}

Stroka TFileNodeProxy::GetFileName()
{
    auto parent = GetParent();
    YASSERT(parent);

    switch (parent->GetType()) {
        case ENodeType::Map:
            return parent->AsMap()->GetChildKey(this);

        case ENodeType::List:
            return ToString(parent->AsList()->GetChildIndex(this));

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TFileNodeProxy, Fetch)
{
    UNUSED(request);

    const auto& impl = GetTypedImpl();
    
    auto chunkListId = impl.GetChunkListId();
    const auto& chunkList = ChunkManager->GetChunkList(chunkListId);
    YASSERT(chunkList.ChildrenIds().size() == 1);
    
    auto chunkId = chunkList.ChildrenIds()[0];
    YASSERT(TypeFromId(chunkId) == EObjectType::Chunk);

    const auto& chunk = ChunkManager->GetChunk(chunkId);

    response->set_chunk_id(chunkId.ToProto());
    ChunkManager->FillHolderAddresses(response->mutable_holder_addresses(), chunk);

    response->set_executable(IsExecutable());
    response->set_file_name(GetFileName());

    context->SetResponseInfo("ChunkId: %s, FileName: %s, Executable: %s, HolderAddresses: [%s]",
        ~chunkId.ToString(),
        ~response->file_name(),
        ~ToString(response->executable()),
        ~JoinToString(response->holder_addresses()));

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

