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

protected:
    virtual void SetDefaultAttributes(
        IAttributeDictionary* attributes,
        TTransaction* transaction) override
    {
        TChunkOwnerTypeHandler::SetDefaultAttributes(attributes, transaction);

        if (!attributes->Contains("compression_codec")) {
            attributes->Set("compression_codec", NCompression::ECodec::None);
        }
    }

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
};

INodeTypeHandlerPtr CreateFileTypeHandler(TBootstrap* bootstrap)
{
    return New<TFileNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

