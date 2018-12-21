#pragma once
#ifndef BLOCK_INL_H_
#error "Direct inclusion of this file is not allowed, include block.h"
// For the sake of sane code completion.
#include "block.h"
#endif

namespace NYT::NChunkClient {

inline TBlock::operator bool() const
{
    return bool(Data);
}

inline size_t TBlock::Size() const
{
    return Data.Size();
}

} // namespace NYT::NChunkClient
