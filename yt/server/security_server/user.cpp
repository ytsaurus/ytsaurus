#include "stdafx.h"
#include "user.h"

#include <server/cell_master/serialization_context.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

TUser::TUser(const TUserId& id)
    : TSubject(id)
{ }

void TUser::Save(NCellMaster::TSaveContext& context) const
{
    TSubject::Save(context);
}

void TUser::Load(NCellMaster::TLoadContext& context)
{
    TSubject::Load(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT

