#include "secondary_index.h"

#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NTableServer;

////////////////////////////////////////////////////////////////////////////////

TString TSecondaryIndex::GetLowercaseObjectName() const
{
    return Format("secondary index %v", GetId());
}

TString TSecondaryIndex::GetCapitalizedObjectName() const
{
    return Format("Secondary index %v", GetId());
}

void TSecondaryIndex::Save(TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, Table_);
    Save(context, IndexTable_);
    Save(context, Kind_);
    Save(context, ExternalCellTag_);
}

void TSecondaryIndex::Load(TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;
    Load(context, Table_);
    Load(context, IndexTable_);
    Load(context, Kind_);
    // COMPAT(sabdenovch)
    if (context.GetVersion() >= EMasterReign::SecondaryIndexReplication) {
        Load(context, ExternalCellTag_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
