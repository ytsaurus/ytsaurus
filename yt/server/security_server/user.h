#pragma once

#include "public.h"
#include "subject.h"

#include <ytlib/misc/property.h>

#include <server/object_server/object.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TUser
    : public TSubject
{
public:
    explicit TUser(const TUserId& id);

    void Save(const NCellMaster::TSaveContext& context) const;
    void Load(const NCellMaster::TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
