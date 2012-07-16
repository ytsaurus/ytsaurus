#include "stdafx.h"
#include "node.h"

namespace NYT {
namespace NCypressClient {

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

} // namespace NCypressClient
} // namespace NYT

