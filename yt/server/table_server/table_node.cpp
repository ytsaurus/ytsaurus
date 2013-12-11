#include "stdafx.h"
#include "table_node.h"
#include "table_node_proxy.h"
#include "private.h"

#include <ytlib/chunk_client/schema.h>

#include <server/chunk_server/chunk.h>
#include <server/chunk_server/chunk_list.h>
#include <server/chunk_server/chunk_owner_type_handler.h>
#include <server/chunk_server/chunk_manager.h>

#include <server/tablet_server/tablet_manager.h>
#include <server/tablet_server/tablet.h>

#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NTableServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NYTree;
using namespace NChunkServer;
using namespace NObjectServer;
using namespace NTableClient;
using namespace NTransactionServer;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

TTableNode::TTableNode(const TVersionedNodeId& id)
    : TChunkOwnerBase(id)
{ }

EObjectType TTableNode::GetObjectType() const
{
    return EObjectType::Table;
}

TTableNode* TTableNode::GetTrunkNode() const
{
    return static_cast<TTableNode*>(TrunkNode_);
}

bool TTableNode::IsSorted() const
{
    return !ChunkList_->SortedBy().empty();
}

void TTableNode::Save(TSaveContext& context) const
{
    TChunkOwnerBase::Save(context);

    SaveObjectRefs(context, Tablets_);
}

void TTableNode::Load(TLoadContext& context)
{
    TChunkOwnerBase::Load(context);

    // COMPAT(babenko)
    if (context.GetVersion() >= 100) {
        LoadObjectRefs(context, Tablets_);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TTableNodeTypeHandler
    : public TChunkOwnerTypeHandler<TTableNode>
{
public:
    typedef TChunkOwnerTypeHandler<TTableNode> TBase;

    explicit TTableNodeTypeHandler(TBootstrap* bootstrap)
        : TBase(bootstrap)
    { }

    virtual void SetDefaultAttributes(
        IAttributeDictionary* attributes,
        TTransaction* transaction) override
    {
        TBase::SetDefaultAttributes(attributes, transaction);

        if (!attributes->Contains("channels")) {
            attributes->SetYson("channels", TYsonString("[]"));
        }

        if (!attributes->Contains("schema")) {
            attributes->SetYson("schema", TYsonString("[]"));
        }

        if (!attributes->Contains("compression_codec")) {
            attributes->SetYson(
                "compression_codec",
                ConvertToYsonString(NCompression::ECodec(NCompression::ECodec::Lz4)));
        }
    }

    virtual EObjectType GetObjectType() override
    {
        return EObjectType::Table;
    }

protected:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TTableNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateTableNodeProxy(
            this,
            Bootstrap,
            transaction,
            trunkNode);
    }

    virtual void DoDestroy(TTableNode* table) override
    {
        TBase::DoDestroy(table);

        tabletManager->UnmountTable(table);
    }
};

INodeTypeHandlerPtr CreateTableTypeHandler(TBootstrap* bootstrap)
{
    return New<TTableNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

