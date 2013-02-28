#include "stdafx.h"
#include "lock.h"

#include <ytlib/misc/serialize.h>

#include <server/cell_master/serialization_context.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

void Save(const NCellMaster::TSaveContext& context, const TLock& lock)
{
    auto* output = context.GetOutput();
    Save(output, lock.Mode);
    SaveSet(output, lock.ChildKeys);
    SaveSet(output, lock.AttributeKeys);
}

void Load(const NCellMaster::TLoadContext& context, TLock& lock)
{
    auto* input = context.GetInput();
    Load(input, lock.Mode);
    LoadSet(input, lock.ChildKeys);
    LoadSet(input, lock.AttributeKeys);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

