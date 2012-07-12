#include "stdafx.h"
#include "node.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

void Save(TOutputStream* output, const TLock& lock)
{
    Save(output, lock.Mode);
}

void Load(TInputStream* input, TLock& lock)
{
    Load(input, lock.Mode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

