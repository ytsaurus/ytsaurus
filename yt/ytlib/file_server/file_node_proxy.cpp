#include "stdafx.h"
#include "file_node_proxy.h"
#include "file_chunk_meta.pb.h"

#include "../misc/string.h"

namespace NYT {
namespace NFileServer {

using namespace NChunkServer;
using namespace NCypress;
using namespace NYTree;
using namespace NRpc;
using namespace NChunkClient;
using namespace NFileClient::NProto;

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

bool TFileNodeProxy::IsLogged(IServiceContext* context) const
{
    Stroka verb = context->GetVerb();
    if (verb == "SetFileChunk") {
        return true;
    } else {
        return TBase::IsLogged(context);
    }
}

void TFileNodeProxy::DoInvoke(IServiceContext* context)
{
    Stroka verb = context->GetVerb();
    if (verb == "GetFileChunk") {
        GetFileChunkThunk(context);
    } else if (verb == "SetFileChunk") {
        SetFileChunkThunk(context);
    } else {
        TBase::DoInvoke(context);
    }
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TFileNodeProxy, GetFileChunk)
{
    UNUSED(request);

    const auto& impl = GetTypedImpl();

    TChunkId chunkId;
    auto chunkListId = impl.GetChunkListId();
    if (chunkListId == NullChunkId) {
        response->set_chunkid(NullChunkId.ToProto());

        context->SetResponseInfo("ChunkId: %s", ~NullChunkId.ToString());
    } else {
        const auto& chunkList = ChunkManager->GetChunkList(chunkListId);
        YASSERT(chunkList.ChunkIds().ysize() == 1);
        chunkId = chunkList.ChunkIds()[0];

        const auto& chunk = ChunkManager->GetChunk(chunkId);

        response->set_chunkid(chunkId.ToProto());
        FOREACH (auto holderId, chunk.Locations()) {
            auto& holder = ChunkManager->GetHolder(holderId);
            response->add_holderaddresses(holder.GetAddress());
        }   

        context->SetResponseInfo("ChunkId: %s, HolderAddresses: [%s]",
            ~chunkId.ToString(),
            ~JoinToString(response->holderaddresses()));
    }

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TFileNodeProxy, SetFileChunk)
{
    UNUSED(response);

    auto chunkId = TChunkId::FromProto(request->chunkid());

    context->SetRequestInfo("ChunkId: %s", ~chunkId.ToString());

    auto& chunk = ChunkManager->GetChunkForUpdate(chunkId);
    if (chunk.GetChunkListId() != NullChunkListId) {
        ythrow yexception() << "Chunk is already assigned to another chunk list";
    }

    EnsureLocked();

    auto& impl = GetTypedImplForUpdate();

    if (impl.GetChunkListId() != NullChunkListId) {
        ythrow yexception() << "File already has a chunk";
    }

    // Create a chunklist and couple it with the chunk.
    auto& chunkList = ChunkManager->CreateChunkList();
    ChunkManager->AddChunkToChunkList(chunk, chunkList);

    // Reference the chunklist from the file.
    impl.SetChunkListId(chunkList.GetId());
    ChunkManager->RefChunkList(chunkList);

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

