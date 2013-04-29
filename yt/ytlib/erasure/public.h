#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/misc/small_vector.h>

#include <bitset>

namespace NYT {
namespace NErasure {

///////////////////////////////////////////////////////////////////////////////

//! The maximum total number of blocks our erasure codec can handle.
const int MaxTotalBlockCount = 16;

//! A vector type for holding block indexes without allocations.
typedef TSmallVector<int, MaxTotalBlockCount> TBlockIndexList;

//! Each bit corresponds to a possible block index.
typedef std::bitset<MaxTotalBlockCount> TBlockIndexSet;

struct ICodec;

DECLARE_ENUM(ECodec,
    ((None)           (0))
    ((ReedSolomon_6_3)(1)) // ReedSolomon_6_3
    ((Lrc_12_2_2)     (2)) // Lrc_12_2_2
);

///////////////////////////////////////////////////////////////////////////////

} // namespace NErasure
} // namespace NYT
