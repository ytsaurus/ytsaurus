#include "stdafx.h"
#include "lock.h"

#include <ytlib/misc/serialize.h>

#include <server/cell_master/serialization_context.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

void Save(NCellMaster::TSaveContext& context, const TLock& lock)
{
    Save(context, lock.Mode);
    Save(context, lock.ChildKeys);
    Save(context, lock.AttributeKeys);
}

void Load(NCellMaster::TLoadContext& context, TLock& lock)
{
    Load(context, lock.Mode);
    Load(context, lock.ChildKeys);
    Load(context, lock.AttributeKeys);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

