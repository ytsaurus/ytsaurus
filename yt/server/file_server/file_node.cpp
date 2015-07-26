#include "stdafx.h"
#include "file_node.h"
#include "file_node_proxy.h"
#include "private.h"

#include <ytlib/file_client/file_ypath_proxy.h>

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
    explicit TFileNodeTypeHandler(TBootstrap* bootstrap)
        : TChunkOwnerTypeHandler(bootstrap)
    { }

    virtual EObjectType GetObjectType() override
    {
        return EObjectType::File;
    }

    virtual bool IsExternalizable() override
    {
        return true;
    }

    virtual void SetDefaultAttributes(
        IAttributeDictionary* attributes,
        TTransaction* transaction) override
    {
        TChunkOwnerTypeHandler::SetDefaultAttributes(attributes, transaction);

        if (!attributes->Contains("compression_codec")) {
            attributes->Set("compression_codec", NCompression::ECodec::None);
        }
    }

protected:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TFileNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateFileNodeProxy(
            this,
            Bootstrap_,
            transaction,
            trunkNode);
    }

    virtual std::unique_ptr<TFileNode> DoCreate(
        const TVersionedNodeId& id,
        TCellTag cellTag,
        TReqCreate* request,
        TRspCreate* response) override
    {
        // NB: Validate everything before calling TBase::DoCreate to ensure atomicity.
        TChunk* chunk = nullptr;
        auto chunkManager = Bootstrap_->GetChunkManager();
        if (request && request->HasExtension(TReqCreateFileExt::create_file_ext)) {
            const auto& requestExt = request->GetExtension(TReqCreateFileExt::create_file_ext);
            auto chunkId = FromProto<TChunkId>(requestExt.chunk_id());
            chunk = chunkManager->GetChunkOrThrow(chunkId);
            chunk->ValidateConfirmed();
        }

        auto nodeHolder = TChunkOwnerTypeHandler::DoCreate(id, cellTag, request, response);

        if (chunk) {
            auto* chunkList = nodeHolder->GetChunkList();
            chunkManager->AttachToChunkList(chunkList, chunk);
        }

        return nodeHolder;
    }

};

INodeTypeHandlerPtr CreateFileTypeHandler(TBootstrap* bootstrap)
{
    return New<TFileNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

