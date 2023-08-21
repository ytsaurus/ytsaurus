#include "group.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NSecurityServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TString TGroup::GetLowercaseObjectName() const
{
    return Format("group %Qv", Name_);
}

TString TGroup::GetCapitalizedObjectName() const
{
    return Format("Group %Qv", Name_);
}

TString TGroup::GetObjectPath() const
{
    return Format("//sys/groups/%v", Name_);
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

