#ifndef PUBLIC_INL_H_
#error "Direct inclusion of this file is not allowed, include public.h"
// For the sake of sane code completion.
#include "public.h"
#endif

#include <array>
#include <span>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

inline constexpr auto SentinelMediumIndexesStorage = [] {
    std::array<int, 2 + (MediumIndexBound - RealMediumIndexBound)> sentinels{
        GenericMediumIndex,
        AllMediaIndex,
    };
    for (int index = RealMediumIndexBound; index < MediumIndexBound; ++index) {
        sentinels[2 + (index - RealMediumIndexBound)] = index;
    }
    return sentinels;
}();

constexpr std::span<const int> GetSentinelMediumIndexes()
{
    return std::span<const int>(SentinelMediumIndexesStorage);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
