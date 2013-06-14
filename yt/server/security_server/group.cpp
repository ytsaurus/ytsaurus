#include "stdafx.h"
#include "group.h"

#include <server/cell_master/serialization_context.h>

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
    
    SaveObjectRefs(context, Members_);
}

void TGroup::Load(NCellMaster::TLoadContext& context)
{
    TSubject::Load(context);

    LoadObjectRefs(context, Members_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT

