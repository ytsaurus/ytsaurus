#include "stdafx.h"
#include "node.h"

namespace NYT {
namespace NCypressServer {

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

} // namespace NCypressServer
} // namespace NYT

