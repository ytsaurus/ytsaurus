#include "group.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NSecurityServer {

using namespace NCellMaster;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

std::string TGroup::GetLowercaseObjectName() const
{
    return Format("group %Qv", Name_);
}

std::string TGroup::GetCapitalizedObjectName() const
{
    return Format("Group %Qv", Name_);
}

TYPath TGroup::GetObjectPath() const
{
    return Format("//sys/groups/%v", NYPath::ToYPathLiteral(GetName()));
}

void TGroup::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TSubject>();

    PHOENIX_REGISTER_FIELD(1, Members_);
}

PHOENIX_DEFINE_TYPE(TGroup);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

