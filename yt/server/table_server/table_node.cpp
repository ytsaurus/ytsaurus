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

using namespace NTableClient;
using namespace NCellMaster;
using namespace NCypressServer;
using namespace NYTree;
using namespace NChunkServer;
using namespace NChunkClient;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NTabletServer;

////////////////////////////////////////////////////////////////////////////////

TTableNode::TTableNode(const TVersionedNodeId& id)
    : TChunkOwnerBase(id)
    , Sorted_(false)
    , Atomicity_(NTransactionClient::EAtomicity::Full)
{ }

EObjectType TTableNode::GetObjectType() const
{
    return EObjectType::Table;
}

TTableNode* TTableNode::GetTrunkNode() const
{
    return static_cast<TTableNode*>(TrunkNode_);
}

void TTableNode::Save(TSaveContext& context) const
{
    TChunkOwnerBase::Save(context);

    using NYT::Save;
    Save(context, Sorted_);
    Save(context, KeyColumns_);
    Save(context, Tablets_);
    Save(context, Atomicity_);
}

void TTableNode::Load(TLoadContext& context)
{
    TChunkOwnerBase::Load(context);

    using NYT::Load;
    // COMPAT(babenko)
    if (context.GetVersion() >= 100) {
        Load(context, Sorted_);
        Load(context, KeyColumns_);
        Load(context, Tablets_);
    }
    // COMPAT(babenko)
    if (context.GetVersion() >= 120) {
        Load(context, Atomicity_);
    }
}

std::pair<TTableNode::TTabletListIterator, TTableNode::TTabletListIterator> TTableNode::GetIntersectingTablets(
    const TOwningKey& minKey,
    const TOwningKey& maxKey)
{
    auto beginIt = std::upper_bound(
        Tablets_.begin(),
        Tablets_.end(),
        minKey,
        [] (const TOwningKey& key, const TTablet* tablet) {
            return key < tablet->GetPivotKey();
        });

    if (beginIt != Tablets_.begin()) {
        --beginIt;
    }

    auto endIt = beginIt;
    while (endIt != Tablets_.end() && maxKey >= (*endIt)->GetPivotKey()) {
        ++endIt;
    }

    return std::make_pair(beginIt, endIt);
}

bool TTableNode::HasMountedTablets() const
{
    for (const auto* tablet : Tablets_) {
        if (tablet->GetState() == ETabletState::Mounting ||
            tablet->GetState() == ETabletState::Mounted)
        {
            return true;
        }
    }
    return false;
}

bool TTableNode::IsDynamic() const
{
    return !GetTrunkNode()->Tablets().empty();
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
            attributes->Set("compression_codec", NCompression::ECodec::Lz4);
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
            Bootstrap_,
            transaction,
            trunkNode);
    }

    virtual void DoDestroy(TTableNode* table) override
    {
        TBase::DoDestroy(table);

        if (table->IsTrunk()) {
            auto tabletManager = Bootstrap_->GetTabletManager();
            tabletManager->ClearTablets(table);
        }
    }

    virtual void DoBranch(
        const TTableNode* originatingNode,
        TTableNode* branchedNode) override
    {
        branchedNode->KeyColumns() = originatingNode->KeyColumns();
        branchedNode->SetSorted(originatingNode->GetSorted());

        TBase::DoBranch(originatingNode, branchedNode);
    }

    virtual void DoMerge(
        TTableNode* originatingNode,
        TTableNode* branchedNode) override
    {
        originatingNode->KeyColumns() = branchedNode->KeyColumns();
        originatingNode->SetSorted(branchedNode->GetSorted());

        TBase::DoMerge(originatingNode, branchedNode);
    }

    virtual void DoClone(
        TTableNode* sourceNode,
        TTableNode* clonedNode,
        NCypressServer::ICypressNodeFactoryPtr factory,
        ENodeCloneMode mode) override
    {
        switch (mode) {
            case ENodeCloneMode::Copy:
                if (sourceNode->IsDynamic()) {
                    THROW_ERROR_EXCEPTION("Cannot copy a dynamic table");
                }
                break;

            case ENodeCloneMode::Move:
                if (sourceNode->HasMountedTablets()) {
                    THROW_ERROR_EXCEPTION("Cannot move a dynamic table with mounted tablets");
                }
                break;

            default:
                YUNREACHABLE();
        }

        TBase::DoClone(sourceNode, clonedNode, factory, mode);

        clonedNode->SetSorted(sourceNode->GetSorted());
        clonedNode->KeyColumns() = sourceNode->KeyColumns();

        if (sourceNode->IsDynamic()) {
            auto objectManager = Bootstrap_->GetObjectManager();
            for (auto* tablet : sourceNode->Tablets()) {
                objectManager->RefObject(tablet);
                clonedNode->Tablets().push_back(tablet);
                tablet->SetTable(clonedNode);
            }
        }
    }

};

INodeTypeHandlerPtr CreateTableTypeHandler(TBootstrap* bootstrap)
{
    return New<TTableNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

