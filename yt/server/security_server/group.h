#pragma once

#include "public.h"
#include "subject.h"

#include <core/misc/property.h>

#include <server/object_server/object.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TGroup
    : public TSubject
{
    typedef yhash_set<TSubject*> TSubjectSet;
    DEFINE_BYREF_RW_PROPERTY(TSubjectSet, Members);

public:
    explicit TGroup(const TGroupId& id);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
