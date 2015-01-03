#pragma once

#include <core/misc/common.h>
#include <core/misc/small_vector.h>

#include <bitset>

namespace NYT {
namespace NErasure {

///////////////////////////////////////////////////////////////////////////////

//! The maximum total number of blocks our erasure codec can handle.
const int MaxTotalPartCount = 16;

//! A vector type for holding block indexes without allocations.
typedef SmallVector<int, MaxTotalPartCount> TPartIndexList;

//! Each bit corresponds to a possible block index.
typedef std::bitset<MaxTotalPartCount> TPartIndexSet;

struct ICodec;

DEFINE_ENUM_WITH_UNDERLYING_TYPE(ECodec, i8,
    ((None)           (0))
    ((ReedSolomon_6_3)(1))
    ((Lrc_12_2_2)     (2))
);

///////////////////////////////////////////////////////////////////////////////

} // namespace NErasure
} // namespace NYT
