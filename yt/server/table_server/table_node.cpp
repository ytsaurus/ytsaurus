#include "table_node.h"
#include "table_node_proxy.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/config.h>

#include <yt/server/chunk_server/chunk.h>
#include <yt/server/chunk_server/chunk_list.h>
#include <yt/server/chunk_server/chunk_manager.h>
#include <yt/server/chunk_server/chunk_owner_type_handler.h>

#include <yt/server/tablet_server/tablet.h>
#include <yt/server/tablet_server/tablet_manager.h>

#include <yt/ytlib/chunk_client/schema.h>

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
    , PreserveSchemaOnWrite_(false)
    , TabletCellBundle_(nullptr)
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
    const TTableSchema& schema,
    bool preserveSchemaOnWrite)
{
    PreserveSchemaOnWrite_ = preserveSchemaOnWrite;
    TableSchema_ = schema;
    TChunkOwnerBase::EndUpload(statistics, schema, preserveSchemaOnWrite);
}

bool TTableNode::IsSorted() const
{
    return TableSchema_.IsSorted();
}

bool TTableNode::IsUniqueKeys() const
{
    return TableSchema_.IsUniqueKeys();
}

void TTableNode::Save(TSaveContext& context) const
{
    TChunkOwnerBase::Save(context);

    using NYT::Save;
    Save(context, TableSchema_);
    Save(context, PreserveSchemaOnWrite_);
    Save(context, Tablets_);
    Save(context, Atomicity_);
    Save(context, TabletCellBundle_);
}

void TTableNode::Load(TLoadContext& context)
{
    // Brief history of changes.
    // In 205 we removed KeyColumns from the snapshot and introduced TableSchema.
    // In 206 we removed Sorted flag from the snapshot.
    
    TChunkOwnerBase::Load(context);

    using NYT::Load;

    // COMPAT(max42)
    bool sorted;
    if (context.GetVersion() < 206) {
        Load(context, sorted);
    }

    // COMPAT(max42)
    TKeyColumns keyColumns;
    if (context.GetVersion() < 205) {
        Load(context, keyColumns);
    } else {
        Load(context, TableSchema_);
    }

    // COMPAT(savrus)
    if (context.GetVersion() >= 301) {
        Load(context, PreserveSchemaOnWrite_);
    }

    Load(context, Tablets_);
    Load(context, Atomicity_);

    // COMPAT(savrus)
    if (context.GetVersion() < 301) {
        // Set PreserveSchemaOnWrite for dynamic tables.
        if (IsDynamic()) {
            PreserveSchemaOnWrite_ = true;
        }
	}
	
    // COMPAT(babenko)
    if (context.GetVersion() >= 400) {
        Load(context, TabletCellBundle_);
    }

    // COMPAT(max42)
    if (context.GetVersion() < 205) {
        // We erase schema from attributes map since it is now a built-in attribute.
        auto& attributesMap = GetMutableAttributes()->Attributes();
        auto tableSchemaAttribute = attributesMap["schema"];
        attributesMap.erase("schema");
        if (IsDynamic()) {
            auto columns = ConvertTo<std::vector<TColumnSchema>>(tableSchemaAttribute);
            for (int index = 0; index < keyColumns.size(); ++index) {
                const auto& columnName = keyColumns[index];
                YCHECK(columns[index].Name == columnName);
                columns[index].SetSortOrder(ESortOrder::Ascending);
            }
            TableSchema_ = TTableSchema(columns, true /* strict */, true /* unique_keys */);
        } else {
            TableSchema_ = TTableSchema::FromKeyColumns(keyColumns);
        }
    }
    
    // COMPAT(max42): In case there are channels associated with a table, we extend the
    // table schema with all columns mentioned in channels and erase the corresponding attribute.
    {
        auto& attributesMap = GetMutableAttributes()->Attributes();
        if (attributesMap.find("channels")) {
            const auto& channels = ConvertTo<TChannels>(attributesMap["channels"]);
            attributesMap.erase("channels");

            auto columns = TableSchema_.Columns();

            yhash_set<Stroka> columnNames;
            for (const auto& column : columns) {
                columnNames.insert(column.Name);
            }

            for (const auto& channel : channels) {
                const auto& channelColumns = channel.GetColumns();
                for (const auto& name : channelColumns) {
                    if (columnNames.insert(name).second) {
                        columns.push_back(TColumnSchema(name, EValueType::Any));
                    }
                }
            }

            TableSchema_ = TTableSchema(columns, false);
        }
    }

    // COMPAT(max42)
    if (context.GetVersion() < 206) {
        YCHECK(!(sorted && !TableSchema_.IsSorted()));
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
        if (tablet->GetState() != ETabletState::Unmounted) {
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

void TTableNode::SetCustomSchema(TTableSchema schema, bool dynamic)
{
    PreserveSchemaOnWrite_ = true;

    // NB: Sorted dynamic tables contain unique keys, set this for user.
    if (dynamic && schema.IsSorted()) {
        schema.MakeUniqueKeys();
    }

    ValidateTableSchemaUpdate(TableSchema_, schema, dynamic, IsEmpty());

    TableSchema_ = std::move(schema);
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

    virtual EObjectType GetObjectType() const override
    {
        return EObjectType::Table;
    }

    virtual bool IsExternalizable() const override
    {
        return true;
    }

protected:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TTableNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateTableNodeProxy(
            Bootstrap_,
            &Metadata_,
            transaction,
            trunkNode);
    }

    virtual std::unique_ptr<TTableNode> DoCreate(
        const TVersionedNodeId& id,
        TCellTag cellTag,
        TTransaction* transaction,
        IAttributeDictionary* attributes) override
    {
        if (!attributes->Contains("compression_codec")) {
            attributes->Set("compression_codec", NCompression::ECodec::Lz4);
        }

        if (!attributes->Contains("optimize_for")) {
            attributes->Set("optimize_for", EOptimizeFor::Lookup);
        }

        if (!attributes->Contains("tablet_cell_bundle")) {
            attributes->Set("tablet_cell_bundle", DefaultTabletCellBundleName);
        }

        bool dynamic = attributes->Get<bool>("dynamic", false);
        attributes->Remove("dynamic");

        auto maybeSchema = attributes->Find<TTableSchema>("schema");
        attributes->Remove("schema");

        if (maybeSchema) {
            ValidateTableSchema(*maybeSchema);
        }

        if (dynamic && !maybeSchema) {
            THROW_ERROR_EXCEPTION("\"schema\" is mandatory for dynamic tables");
        }

        TBase::InitializeAttributes(attributes);

        auto nodeHolder = TChunkOwnerTypeHandler::DoCreate(
            id,
            cellTag,
            transaction,
            attributes);
        auto* node = nodeHolder.get();

        if (maybeSchema) {
            node->SetCustomSchema(*maybeSchema, dynamic);
        }

        if (dynamic) {
            auto tabletManager = Bootstrap_->GetTabletManager();
            tabletManager->MakeTableDynamic(nodeHolder.get());
        }

        return nodeHolder;
    }

    virtual void DoDestroy(TTableNode* table) override
    {
        TBase::DoDestroy(table);

        if (table->IsTrunk()) {
            auto tabletManager = Bootstrap_->GetTabletManager();
            tabletManager->ClearTablets(table);
            tabletManager->ResetTabletCellBundle(table);
        }
    }

    virtual void DoBranch(
        const TTableNode* originatingNode,
        TTableNode* branchedNode,
        ELockMode mode) override
    {
        branchedNode->TableSchema() = originatingNode->TableSchema();
        branchedNode->SetPreserveSchemaOnWrite(originatingNode->GetPreserveSchemaOnWrite());

        TBase::DoBranch(originatingNode, branchedNode, mode);
    }

    virtual void DoMerge(
        TTableNode* originatingNode,
        TTableNode* branchedNode) override
    {
        originatingNode->TableSchema() = branchedNode->TableSchema();
        originatingNode->SetPreserveSchemaOnWrite(branchedNode->GetPreserveSchemaOnWrite());

        TBase::DoMerge(originatingNode, branchedNode);
    }

    virtual void DoClone(
        TTableNode* sourceNode,
        TTableNode* clonedNode,
        ICypressNodeFactory* factory,
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
        clonedNode->SetPreserveSchemaOnWrite(sourceNode->GetPreserveSchemaOnWrite());

        auto* trunkSourceNode = sourceNode->GetTrunkNode();

        auto tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->SetTabletCellBundle(clonedNode, trunkSourceNode->GetTabletCellBundle());

        if (sourceNode->IsDynamic()) {
            auto tablets = std::move(trunkSourceNode->Tablets());
            factory->RegisterCommitHandler([clonedNode, tablets] () mutable {
                clonedNode->Tablets() = std::move(tablets);
                for (auto* tablet : clonedNode->Tablets()) {
                    tablet->SetTable(clonedNode);
                }
            });
            factory->RegisterRollbackHandler([trunkSourceNode, tablets] () mutable {
                trunkSourceNode->Tablets() = std::move(tablets);
            });
        }
    }

    virtual int GetDefaultReplicationFactor() const override
    {
        return Bootstrap_->GetConfig()->CypressManager->DefaultTableReplicationFactor;
    }
};

INodeTypeHandlerPtr CreateTableTypeHandler(TBootstrap* bootstrap)
{
    return New<TTableNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

