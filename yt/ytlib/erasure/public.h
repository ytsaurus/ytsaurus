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

//! Each bit corresponds to a possible block index.
typedef ui32 TBlockIndexSet;

struct ICodec;

DECLARE_ENUM(ECodec,
    ((None)           (0))
    ((ReedSolomon_6_3)(1))
    ((Lrc_12_2_2)     (2))
);

///////////////////////////////////////////////////////////////////////////////

} // namespace NErasure
} // namespace NYT
