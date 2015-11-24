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
using namespace NYson;
using namespace NChunkServer;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NTabletServer;

////////////////////////////////////////////////////////////////////////////////

TTableNode::TTableNode(const TVersionedNodeId& id)
    : TChunkOwnerBase(id)
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

void TTableNode::BeginUpload(EUpdateMode mode)
{
    TChunkOwnerBase::BeginUpload(mode);
}

void TTableNode::EndUpload(
    const TDataStatistics* statistics,
    bool deriveStatistics,
    const std::vector<Stroka>& keyColumns)
{
    TChunkOwnerBase::EndUpload(statistics, deriveStatistics, keyColumns);
    if (!keyColumns.empty()) {
        TableSchema_ = TTableSchema::FromKeyColumns(keyColumns);
    } else {
        // Table schema columns will be reset, strict = false.
        TableSchema_ = TTableSchema();
    }
}

bool TTableNode::IsSorted() const
{
    return TableSchema_.IsSorted();
}

void TTableNode::Save(TSaveContext& context) const
{
    TChunkOwnerBase::Save(context);

    using NYT::Save;
    Save(context, TableSchema_);
    Save(context, Tablets_);
    Save(context, Atomicity_);
}

void TTableNode::Load(TLoadContext& context)
{
    TChunkOwnerBase::Load(context);

    using NYT::Load;
    
    // COMPAT(max42)
    bool sorted;
    if (context.GetVersion() < 206) {
        Load(context, sorted); 
    } 

    // COMPAT(max42)
    TKeyColumns keyColumns;
    if (context.GetVersion() >= 205) {
        Load(context, TableSchema_);
    } else {
        Load(context, keyColumns);
    }
    
    Load(context, Tablets_);

    // COMPAT(max42)
    if (context.GetVersion() < 205) {
        auto& attributesMap = GetMutableAttributes()->Attributes();
        auto tableSchemaAttribute = attributesMap["schema"];
        attributesMap.erase("schema");
        if (IsDynamic()) {
            TableSchema_ = ConvertTo<TTableSchema>(tableSchemaAttribute);
            for (const auto& columnName : keyColumns) {
                auto columnSchema = TableSchema_.FindColumn(columnName);
                YCHECK(columnSchema);
                columnSchema->SortOrder = ESortOrder::Ascending;
            }
        } else {
            TableSchema_ = TTableSchema::FromKeyColumns(keyColumns);
        }
    }
        
    // COMPAT(max42)
    if (context.GetVersion() < 206) {
        YCHECK(!(sorted && !TableSchema_.IsSorted()));
    }

    Load(context, Atomicity_);
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

bool TTableNode::IsEmpty() const
{
    return ComputeTotalStatistics().chunk_count() == 0;
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

    virtual EObjectType GetObjectType() override
    {
        return EObjectType::Table;
    }

    virtual bool IsExternalizable() override
    {
        return true;
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

    virtual std::unique_ptr<TTableNode> DoCreate(
        const TVersionedNodeId& id,
        TCellTag cellTag,
        TTransaction* transaction,
        IAttributeDictionary* attributes) override
    {
        if (!attributes->Contains("channels")) {
            attributes->SetYson("channels", TYsonString("[]"));
        } 

        if (!attributes->Contains("compression_codec")) {
            attributes->Set("compression_codec", NCompression::ECodec::Lz4);
        }

        TBase::InitializeAttributes(attributes);

        return TChunkOwnerTypeHandler::DoCreate(
            id,
            cellTag,
            transaction,
            attributes);
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
        TTableNode* branchedNode,
        ELockMode mode) override
    {
        branchedNode->TableSchema() = originatingNode->TableSchema();

        TBase::DoBranch(originatingNode, branchedNode, mode);
    }

    virtual void DoMerge(
        TTableNode* originatingNode,
        TTableNode* branchedNode) override
    {
        originatingNode->TableSchema() = branchedNode->TableSchema();

        TBase::DoMerge(originatingNode, branchedNode);
    }

    virtual void DoClone(
        TTableNode* sourceNode,
        TTableNode* clonedNode,
        ICypressNodeFactoryPtr factory,
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

        clonedNode->TableSchema() = sourceNode->TableSchema();

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

