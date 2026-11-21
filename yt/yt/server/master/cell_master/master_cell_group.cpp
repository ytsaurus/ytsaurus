#include "master_cell_group.h"

#include "serialize.h"

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NCellMaster {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

bool TMasterCellGroup::Contains(NObjectClient::TCellTag cellTag) const
{
    return CellTags_.contains(cellTag);
}

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

void TMasterCellGroup::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TObject>();

    PHOENIX_REGISTER_FIELD(1, Name_);
    PHOENIX_REGISTER_FIELD(2, CellTags_);
}

PHOENIX_DEFINE_TYPE(TMasterCellGroup);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
