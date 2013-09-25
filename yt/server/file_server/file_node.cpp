#include "stdafx.h"
#include "file_node.h"
#include "file_node_proxy.h"
#include "private.h"

#include <ytlib/file_client/file_ypath_proxy.h>

#include <server/chunk_server/chunk.h>
#include <server/chunk_server/chunk_list.h>
#include <server/chunk_server/chunk_manager.h>
#include <server/chunk_server/chunk_owner_type_handler.h>

#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NFileServer {

using namespace NCellMaster;
using namespace NYTree;
using namespace NCypressServer;
using namespace NChunkServer;
using namespace NTransactionServer;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NFileClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TFileNode::TFileNode(const TVersionedNodeId& id)
    : TChunkOwnerBase(id)
{ }

////////////////////////////////////////////////////////////////////////////////

class TFileNodeTypeHandler
    : public TChunkOwnerTypeHandler<TFileNode>
{
public:
    typedef TChunkOwnerTypeHandler<TFileNode> TBase;

    explicit TFileNodeTypeHandler(TBootstrap* bootstrap)
        : TBase(bootstrap)
    { }

    virtual EObjectType GetObjectType() override
    {
        return EObjectType::File;
    }

protected:

    virtual void SetDefaultAttributes(
        IAttributeDictionary* attributes,
        TTransaction* transaction) override
    {
        TBase::SetDefaultAttributes(attributes, transaction);

        if (!attributes->Contains("compression_codec")) {
            NCompression::ECodec codec = NCompression::ECodec::None;
            attributes->SetYson(
                "compression_codec",
                TYsonString(FormatEnum(codec)));
        }
    }

    virtual ICypressNodeProxyPtr DoGetProxy(
        TFileNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateFileNodeProxy(
            this,
            Bootstrap,
            transaction,
            trunkNode);
    }

    virtual std::unique_ptr<TFileNode> DoCreate(
        const NCypressServer::TVersionedNodeId& id,
        NTransactionServer::TTransaction* transaction,
        TReqCreate* request,
        TRspCreate* response) override
    {
        auto node = TBase::DoCreate(id, transaction, request, response);

        if (request->HasExtension(TReqCreateFileExt::create_file_ext)) {
            const auto& requestExt = request->GetExtension(TReqCreateFileExt::create_file_ext);
            auto chunkId = FromProto<TChunkId>(requestExt.chunk_id());

            auto chunkManager = Bootstrap->GetChunkManager();
            auto* chunk = chunkManager->GetChunkOrThrow(chunkId);
            if (!chunk->IsConfirmed()) {
                THROW_ERROR_EXCEPTION("Chunk %s is not confirmed", ~ToString(chunkId));
            }

            auto* chunkList = node->GetChunkList();
            chunkManager->AttachToChunkList(chunkList, chunk);
        }

        return node;
    }

};

INodeTypeHandlerPtr CreateFileTypeHandler(NCellMaster::TBootstrap* bootstrap)
{
    return New<TFileNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

