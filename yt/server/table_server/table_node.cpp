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
    , CommitOrdering_(NTransactionClient::ECommitOrdering::Weak)
    , RetainedTimestamp_(NTransactionClient::NullTimestamp)
    , UnflushedTimestamp_(NTransactionClient::NullTimestamp)
{ }

EObjectType TTableNode::GetObjectType() const
{
    return EObjectType::Table;
}

TTableNode* TTableNode::GetTrunkNode()
{
    return TrunkNode_->As<TTableNode>();
}

const TTableNode* TTableNode::GetTrunkNode() const
{
    return TrunkNode_->As<TTableNode>();
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
    Save(context, CommitOrdering_);
    Save(context, TabletCellBundle_);
    Save(context, LastCommitTimestamp_);
    Save(context, RetainedTimestamp_);
    Save(context, UnflushedTimestamp_);
    Save(context, ReplicationMode_);
}

void TTableNode::Load(NCellMaster::TLoadContext& context)
{
    TChunkOwnerBase::Load(context);

    using NYT::Load;
    Load(context, TableSchema_);
    Load(context, SchemaMode_);
    Load(context, Tablets_);
    Load(context, Atomicity_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 400) {
        Load(context, CommitOrdering_);
        Load(context, TabletCellBundle_);
        Load(context, LastCommitTimestamp_);
        Load(context, RetainedTimestamp_);
        Load(context, UnflushedTimestamp_);
    }
    // COMPAT(babenko)
    if (context.GetVersion() >= 508) {
        Load(context, ReplicationMode_);
    }
    // COMPAT(babenko): Cf. YT-5045
    if (Attributes_ && Attributes_->Attributes().empty()) {
        Attributes_.reset();
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

TTimestamp TTableNode::GetCurrentUnflushedTimestamp() const
{
    return UnflushedTimestamp_ != NullTimestamp
        ? UnflushedTimestamp_
        : CalculateUnflushedTimestamp();
}

TTimestamp TTableNode::GetCurrentRetainedTimestamp() const
{
    return RetainedTimestamp_ != NullTimestamp
        ? RetainedTimestamp_
        : CalculateRetainedTimestamp();
}

const TTableNode::TTabletList& TTableNode::Tablets() const
{
    return const_cast<TTableNode*>(this)->Tablets();
}

TTableNode::TTabletList& TTableNode::Tablets()
{
    Y_ASSERT(IsTrunk());
    return Tablets_;
}

TTimestamp TTableNode::CalculateUnflushedTimestamp() const
{
    auto trunkNode = GetTrunkNode();
    auto result = MaxTimestamp;
    for (const auto* tablet : trunkNode->Tablets()) {
        auto timestamp = static_cast<TTimestamp>(tablet->NodeStatistics().unflushed_timestamp());
        result = std::min(result, timestamp);
    }
    return result;
}

TTimestamp TTableNode::CalculateRetainedTimestamp() const
{
    auto trunkNode = GetTrunkNode();
    auto result = MinTimestamp;
    for (const auto* tablet : trunkNode->Tablets()) {
        auto timestamp = tablet->GetRetainedTimestamp();
        result = std::max(result, timestamp);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

