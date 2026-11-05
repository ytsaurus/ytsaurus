#include "master_cell_group.h"

#include "serialize.h"

#include <yt/yt/core/ypath/token.h>

namespace NYT::NCellMaster {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

std::string TMasterCellGroup::GetLowercaseObjectName() const
{
    return Format("master cell group %Qv", GetName());
}

std::string TMasterCellGroup::GetCapitalizedObjectName() const
{
    return Format("Master cell group %Qv", GetName());
}

TYPath TMasterCellGroup::GetObjectPath() const
{
    return Format("//sys/master_cell_groups/%v", ToYPathLiteral(GetName()));
}

void TMasterCellGroup::Save(NCellMaster::TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, Name_);
    Save(context, CellTags_);
}

void TMasterCellGroup::Load(NCellMaster::TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;
    Load(context, Name_);
    Load(context, CellTags_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
