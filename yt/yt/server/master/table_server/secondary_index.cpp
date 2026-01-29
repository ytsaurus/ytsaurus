#include "secondary_index.h"

#include <yt/yt/server/master/table_server/table_node.h>

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NSecurityServer;
using namespace NTableServer;
using namespace NTabletClient;
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
    Save(context, UnfoldedColumns_);
    Save(context, TableToIndexCorrespondence_);
    TNullableIntrusivePtrSerializer<>::Save(context, EvaluatedColumnsSchema_);
}

void TSecondaryIndex::Load(TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;
    Load(context, TableId_);
    Load(context, IndexTableId_);
    Load(context, Kind_);
    Load(context, ExternalCellTag_);
    Load(context, Predicate_);

    // COMPAT(sabdenovch)
    if (context.GetVersion() >= EMasterReign::SecondaryIndexUnfoldedNames) {
        Load(context, UnfoldedColumns_);
    } else {
        if (auto unfoldedColumn = Load<std::optional<std::string>>(context)) {
            UnfoldedColumns_ = TUnfoldedColumns{
                .TableColumn = *unfoldedColumn,
                .IndexColumn = *unfoldedColumn,
            };
        }
    }

    Load(context, TableToIndexCorrespondence_);

    // COMPAT(sabdenovch)
    if (context.GetVersion() >= EMasterReign::SecondaryIndexEvaluated) {
        TNullableIntrusivePtrSerializer<>::Load(context, EvaluatedColumnsSchema_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
