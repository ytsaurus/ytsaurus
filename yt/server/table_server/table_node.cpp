#include "table_node.h"
#include "private.h"

#include <yt/server/tablet_server/tablet.h>
#include <yt/server/tablet_server/tablet_cell_bundle.h>

#include <yt/ytlib/chunk_client/schema.h>

namespace NYT {
namespace NTableServer {

using namespace NTableClient;
using namespace NCypressServer;
using namespace NYTree;
using namespace NChunkServer;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NTabletServer;

////////////////////////////////////////////////////////////////////////////////

TTableNode::TTableNode(const TVersionedNodeId& id)
    : TChunkOwnerBase(id)
    , SchemaMode_(ETableSchemaMode::Weak)
    , LastCommitTimestamp_(NullTimestamp)
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
    ETableSchemaMode schemaMode)
{
    SchemaMode_ = schemaMode;
    TableSchema_ = schema;
    TChunkOwnerBase::EndUpload(statistics, schema, schemaMode);
}

bool TTableNode::IsSorted() const
{
    return TableSchema_.IsSorted();
}

bool TTableNode::IsUniqueKeys() const
{
    return TableSchema_.IsUniqueKeys();
}

bool TTableNode::IsReplicated() const
{
    return GetObjectType() == EObjectType::ReplicatedTable;
}

bool TTableNode::IsPhysicallySorted() const
{
    return IsSorted() && !IsReplicated();
}

ETabletState TTableNode::GetTabletState() const
{
    auto result = ETabletState::None;
    for (const auto* tablet : GetTrunkNode()->Tablets_) {
        auto state = tablet->GetState();
        if (result == ETabletState::None) {
            result = state;
        } else if (result != state) {
            result = ETabletState::Mixed;
        }
    }
    return result;
}

void TTableNode::Save(NCellMaster::TSaveContext& context) const
{
    TChunkOwnerBase::Save(context);

    using NYT::Save;
    Save(context, TableSchema_);
    Save(context, SchemaMode_);
    Save(context, Tablets_);
    Save(context, Atomicity_);
    Save(context, TabletCellBundle_);
    Save(context, LastCommitTimestamp_);
}

void TTableNode::Load(NCellMaster::TLoadContext& context)
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
    if (context.GetVersion() >= 350) {
        Load(context, SchemaMode_);
    }

    Load(context, Tablets_);
    Load(context, Atomicity_);

    // COMPAT(savrus)
    if (context.GetVersion() < 350) {
        // Set SchemaMode for dynamic tables.
        if (IsDynamic()) {
            SchemaMode_ = ETableSchemaMode::Strong;
        }
	}
	
    // COMPAT(babenko)
    if (context.GetVersion() >= 400) {
        Load(context, TabletCellBundle_);
    }

    // COMAPT(babenko)
    if (context.GetVersion() >= 404) {
        Load(context, LastCommitTimestamp_);
    }

    // COMPAT(max42)
    if (context.GetVersion() < 205 && Attributes_) {
        // We erase schema from attributes map since it is now a built-in attribute.
        auto& attributesMap = Attributes_->Attributes();
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
    if (context.GetVersion() < 205 && Attributes_) {
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

    // COMPAT(babenko): Cf. YT-5045
    if (Attributes_ && Attributes_->Attributes().empty()) {
        Attributes_.reset();
    }

    // COMPAT(max42)
    if (context.GetVersion() < 206) {
        YCHECK(!(sorted && !TableSchema_.IsSorted()));
    }

    // COMPAT(savrus) See YT-5031
    if (context.GetVersion() < 301) {
        if (IsDynamic() && !TableSchema_.GetStrict()) {
            TableSchema_ = TTableSchema(TableSchema_.Columns(), true /* strict */);
        }
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

bool TTableNode::IsDynamic() const
{
    return !GetTrunkNode()->Tablets().empty();
}

bool TTableNode::IsEmpty() const
{
    return ComputeTotalStatistics().chunk_count() == 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

