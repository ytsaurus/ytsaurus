#include "group.h"

#include <yt/server/master/cell_master/serialize.h>

namespace NYT::NSecurityServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TGroup::TGroup(TGroupId id)
    : TSubject(id)
{ }

TString TGroup::GetLowercaseObjectName() const
{
    return Format("group %Qv", Name_);
}

TString TGroup::GetCapitalizedObjectName() const
{
    return Format("Group %Qv", Name_);
}

void TGroup::Save(NCellMaster::TSaveContext& context) const
{
    TSubject::Save(context);

    using NYT::Save;
    Save(context, Members_);
}

void TGroup::Load(NCellMaster::TLoadContext& context)
{
    TSubject::Load(context);

    using NYT::Load;
    Load(context, Members_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

