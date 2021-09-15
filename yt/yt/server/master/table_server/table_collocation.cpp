#include "table_collocation.h"

#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NTableServer;

////////////////////////////////////////////////////////////////////////////////

TString TTableCollocation::GetLowercaseObjectName() const
{
    return Format("table collocation %v", GetId());
}

TString TTableCollocation::GetCapitalizedObjectName() const
{
    return Format("Table collocation %v", GetId());
}

void TTableCollocation::Save(TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, ExternalCellTag_);
    Save(context, Tables_);
    Save(context, Type_);
}

void TTableCollocation::Load(TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;
    Load(context, ExternalCellTag_);
    Load(context, Tables_);
    Load(context, Type_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
