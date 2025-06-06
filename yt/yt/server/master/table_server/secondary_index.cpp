#include "secondary_index.h"

#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NSecurityServer;
using namespace NTableServer;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

std::string TSecondaryIndex::GetLowercaseObjectName() const
{
    return Format("secondary index %v", GetId());
}

std::string TSecondaryIndex::GetCapitalizedObjectName() const
{
    return Format("Secondary index %v", GetId());
}

void TSecondaryIndex::Save(TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, TableId_);
    Save(context, IndexTableId_);
    Save(context, Kind_);
    Save(context, ExternalCellTag_);
    Save(context, Predicate_);
    Save(context, UnfoldedColumn_);
    Save(context, TableToIndexCorrespondence_);
    TNullableIntrusivePtrSerializer<>::Save(context, EvaluatedColumnsSchema_);
}

void TSecondaryIndex::Load(TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;
    // COMPAT(sabdenovch)
    if (context.GetVersion() >= EMasterReign::SecondaryIndexExternalCellTag) {
        Load(context, TableId_);
        Load(context, IndexTableId_);
    } else {
        Load(context, CompatTable_);
        Load(context, CompatIndexTable_);
    }
    Load(context, Kind_);
    Load(context, ExternalCellTag_);
    // COMPAT(sabdenovch)
    if (context.GetVersion() >= EMasterReign::SecondaryIndexPredicate) {
        Load(context, Predicate_);
    }
    // COMPAT(sabdenovch)
    // For older snapshots, this field is filled in TTableManager::OnAfterSnapshotLoaded.
    if (context.GetVersion() >= EMasterReign::SecondaryIndexUnfoldedColumnApi) {
        Load(context, UnfoldedColumn_);
    }
    // COMPAT(sabdenovch)
    if (context.GetVersion() >= EMasterReign::SecondaryIndexStates) {
        Load(context, TableToIndexCorrespondence_);
    } else {
        TableToIndexCorrespondence_ = ETableToIndexCorrespondence::Unknown;
    }

    // COMPAT(sabdenovch)
    if (context.GetVersion() >= EMasterReign::SecondaryIndexEvaluated) {
        TNullableIntrusivePtrSerializer<>::Load(context, EvaluatedColumnsSchema_);
    }
}

void TSecondaryIndex::SetIdsFromCompat()
{
    TableId_ = CompatTable_->GetId();
    IndexTableId_ = CompatIndexTable_->GetId();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
