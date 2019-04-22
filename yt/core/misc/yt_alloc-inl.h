#pragma once
#ifndef YT_ALLOC_INL_H_
#error "Direct inclusion of this file is not allowed, include yt_alloc.h"
// For the sake of sane code completion.
#include "yt_alloc.h"
#endif

#include <yt/core/misc/size_literals.h>

namespace NYT::NYTAlloc {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t TaggedSmallChunkHeaderSize = 16;
constexpr size_t LargeSizeThreshold = 32_KB;

////////////////////////////////////////////////////////////////////////////////

// Maps small chunk ranks to size in bytes.
constexpr ui16 SmallRankToSize[SmallRankCount] = {
    0,
    16, 16,
    32, 32, 48, 64, 96, 128,
    192, 256, 384, 512, 768, 1024, 1536, 2048,
    3072, 4096, 6144, 8192, 12288, 16384, 24576, 32768,
};

// Helper array for mapping size to small chunk rank.
constexpr ui8 SizeToSmallRank1[65] = {
    1,
    2, 2, 4, 4,  // 16, 16, 32, 32
    5, 5, 6, 6,  // 48, 64
    7, 7, 7, 7, 8, 8, 8, 8, // 96, 128
    9, 9, 9, 9, 9, 9, 9, 9,  10, 10, 10, 10, 10, 10, 10, 10,  // 192, 256
    11, 11, 11, 11, 11, 11, 11, 11,  11, 11, 11, 11, 11, 11, 11, 11,  // 384
    12, 12, 12, 12, 12, 12, 12, 12,  12, 12, 12, 12, 12, 12, 12, 12   // 512
};

// Helper array for mapping size to small chunk rank.
constexpr unsigned char SizeToSmallRank2[128] = {
    12, 12, 13, 14, // 512, 512, 768, 1024
    15, 15, 16, 16, // 1536, 2048
    17, 17, 17, 17, 18, 18, 18, 18, // 3072, 4096
    19, 19, 19, 19, 19, 19, 19, 19,  20, 20, 20, 20, 20, 20, 20, 20, // 6144, 8192
    21, 21, 21, 21, 21, 21, 21, 21,  21, 21, 21, 21, 21, 21, 21, 21, // 12288
    22, 22, 22, 22, 22, 22, 22, 22,  22, 22, 22, 22, 22, 22, 22, 22, // 16384
    23, 23, 23, 23, 23, 23, 23, 23,  23, 23, 23, 23, 23, 23, 23, 23,
    23, 23, 23, 23, 23, 23, 23, 23,  23, 23, 23, 23, 23, 23, 23, 23, // 24576
    24, 24, 24, 24, 24, 24, 24, 24,  24, 24, 24, 24, 24, 24, 24, 24,
    24, 24, 24, 24, 24, 24, 24, 24,  24, 24, 24, 24, 24, 24, 24, 24, // 32768
};

////////////////////////////////////////////////////////////////////////////////

constexpr size_t SizeToSmallRank(size_t size)
{
    if (size <= 512) {
        return SizeToSmallRank1[1 + ((static_cast<int>(size) - 1) >> 3)];
    } else {
        if (size < LargeSizeThreshold) {
            return SizeToSmallRank2[(size - 1) >> 8];
        } else {
            return 0;
        }
    }
}

void* AllocateSmall(size_t untaggedRank, size_t taggedRank);

template <size_t Size>
void* AllocateConstSize()
{
    constexpr auto untaggedRank = SizeToSmallRank(Size);
    constexpr auto taggedRank = SizeToSmallRank(Size + TaggedSmallChunkHeaderSize);
    if (untaggedRank != 0 && taggedRank != 0) {
        return AllocateSmall(untaggedRank, taggedRank);
    } else {
        return Allocate(Size);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTAlloc
