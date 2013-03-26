#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/misc/small_vector.h>

namespace NYT {
namespace NErasure {

///////////////////////////////////////////////////////////////////////////////

//! The maximum total number of blocks our erasure codec can handle.
const int MaxTotalBlockCount = 16;

//! A vector type for holding block indexes without allocations.
typedef TSmallVector<int, MaxTotalBlockCount> TBlockIndexList; 

struct ICodec;

///////////////////////////////////////////////////////////////////////////////

} // namespace NErasure
} // namespace NYT
