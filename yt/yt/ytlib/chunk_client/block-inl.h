#ifndef BLOCK_INL_H_
#error "Direct inclusion of this file is not allowed, include block.h"
// For the sake of sane code completion.
#include "block.h"
#endif

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

inline TBlock::operator bool() const
{
    return static_cast<bool>(Data);
}

inline i64 TBlock::Size() const
{
    return static_cast<i64>(Data.Size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
