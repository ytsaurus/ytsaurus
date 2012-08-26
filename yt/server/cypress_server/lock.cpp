#include "stdafx.h"
#include "lock.h"

#include <ytlib/misc/serialize.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

void Save(TOutputStream* output, const TLock& lock)
{
    Save(output, lock.Mode);
    SaveSet(output, lock.ChildKeys);
    SaveSet(output, lock.AttributeKeys);
}

void Load(TInputStream* input, TLock& lock)
{
    Load(input, lock.Mode);
    LoadSet(input, lock.ChildKeys);
    LoadSet(input, lock.AttributeKeys);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

