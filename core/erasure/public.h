#pragma once

#include <yt/core/misc/public.h>
#include <yt/core/misc/small_vector.h>

#include <bitset>

namespace NYT::NErasure {

////////////////////////////////////////////////////////////////////////////////

//! The maximum total number of blocks our erasure codec can handle.
constexpr int MaxTotalPartCount = 16;

//! A vector type for holding block indexes without allocations.
using TPartIndexList = SmallVector<int, MaxTotalPartCount>;

//! Each bit corresponds to a possible block index.
using TPartIndexSet = std::bitset<MaxTotalPartCount>;

struct ICodec;

DEFINE_ENUM_WITH_UNDERLYING_TYPE(ECodec, i8,
    ((None)           (0))
    ((ReedSolomon_6_3)(1))
    ((Lrc_12_2_2)     (2))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NErasure
