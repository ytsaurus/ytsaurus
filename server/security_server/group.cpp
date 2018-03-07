#include "group.h"

#include <yt/server/cell_master/serialize.h>

namespace NYT {
namespace NSecurityServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TGroup::TGroup(const TGroupId& id)
    : TSubject(id)
{ }

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

} // namespace NSecurityServer
} // namespace NYT

