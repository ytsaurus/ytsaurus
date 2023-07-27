#pragma once

#include "public.h"

#include <library/cpp/yt/assert/assert.h>

#include <util/generic/ptr.h>
#include <util/generic/singleton.h>
#include <util/stream/output.h>

extern "C" {
#include <contrib/libs/jerasure/jerasure.h>
}

namespace NErasure {

namespace NPrivate {

struct TJerasureScheduleDeleter {
    template <class T>
    static void Destroy(T** schedulePtr) {
        jerasure_free_schedule(schedulePtr);
    }
};

struct TJerasureInitializer {
    TJerasureInitializer() {
        for (size_t w = 1; w <= MaxWordSize; ++w) {
            galois_create_log_tables(w);
        }
        for (size_t w = 1; w <= MaxWordSize; ++w) {
            galois_create_mult_tables(w);
        }
    }
};

}

using TJerasureScheduleHolder = THolder<int*, NPrivate::TJerasureScheduleDeleter>;

//! TFree because jerasure_matrix_to_bitmatrix creates the object with malloc
using TJerasureMatrixHolder = THolder<int, TFree>;

//! Must be invoked prior to calling jerasure or galois functions to ensure
//! thread-safe initialization.
/*!
 *  Note that the tables are only initialized for word sizes up to #MaxWordSize.
 *  Don't use sizes exceeding this limit without adjusting the constant.
 */
static inline void InitializeJerasure() {
    Singleton<NPrivate::TJerasureInitializer>();
}

template <int BlockCount, int ParityCount, int WordSize, class TCodecTraits, class TBlobType = typename TCodecTraits::TBlobType, class TMutableBlobType = typename TCodecTraits::TMutableBlobType>
std::vector<TBlobType> ScheduleEncode(
    const TJerasureScheduleHolder& schedule,
    const std::vector<TBlobType>& dataBlocks)
{
    YT_VERIFY(dataBlocks.size() == BlockCount);

    size_t blockLength = dataBlocks.front().Size();
    // We should multiply WordSize by 8 because we use BitMatrix
    YT_VERIFY(blockLength % (WordSize * sizeof(long)) == 0);

    for (size_t i = 1; i < dataBlocks.size(); ++i) {
        YT_VERIFY(dataBlocks[i].Size() == blockLength);
    }

    std::vector<char*> dataPointers;
    dataPointers.reserve(dataBlocks.size());
    for (const TBlobType& block : dataBlocks) {
        dataPointers.push_back(const_cast<char*>(reinterpret_cast<const char*>(block.Begin())));
    }

    std::vector<TMutableBlobType> parities(ParityCount);
    std::vector<char*> parityPointers(ParityCount);
    for (size_t i = 0; i < ParityCount; ++i) {
        parities[i] = TCodecTraits::AllocateBlob(blockLength);
        parityPointers[i] = const_cast<char*>(reinterpret_cast<const char*>(parities[i].Begin()));
    }

    jerasure_schedule_encode(
        BlockCount,
        ParityCount,
        WordSize,
        schedule.Get(),
        dataPointers.data(),
        parityPointers.data(),
        blockLength,
        sizeof(long));

    return std::vector<TBlobType>(parities.begin(), parities.end());
}

template <int BlockCount, int WordSize, class TCodecTraits, class TBlobType = typename TCodecTraits::TBlobType, class TMutableBlobType = typename TCodecTraits::TMutableBlobType>
std::vector<TBlobType> BitMatrixDecode(
    int parityCount,
    const TJerasureMatrixHolder& bitMatrix,
    const std::vector<TBlobType>& blocks,
    const TPartIndexList& erasedIndices)
{
    YT_VERIFY(blocks.size() + erasedIndices.size() == static_cast<size_t>(BlockCount + parityCount));

    size_t blockLength = blocks.front().Size();
    // We should multiply WordSize by 8 because we use BitMatrix
    YT_VERIFY(blockLength % (WordSize * sizeof(long)) == 0);

    // all blocks must be of same length
    for (size_t i = 1; i < blocks.size(); ++i) {
        YT_VERIFY(blocks[i].Size() == blockLength);
    }

    std::vector<TMutableBlobType> repaired(erasedIndices.size());

    std::vector<char*> blockPointers;
    blockPointers.reserve(BlockCount);

    std::vector<char*> parityPointers;
    parityPointers.reserve(parityCount);

    size_t blockNumber = 0;
    size_t erasureNumber = 0;
    for (int i = 0; i < BlockCount + parityCount; ++i) {
        char* ptr;
        if (erasureNumber < erasedIndices.size() && i == erasedIndices[erasureNumber]) {
            repaired[erasureNumber] = TCodecTraits::AllocateBlob(blockLength);
            ptr = const_cast<char*>(reinterpret_cast<const char*>(repaired[erasureNumber].Begin()));
            ++erasureNumber;
        } else {
            ptr = const_cast<char*>(reinterpret_cast<const char*>(blocks[blockNumber++].Begin()));
        }
        if (i < BlockCount) {
            blockPointers.push_back(ptr);
        } else {
            parityPointers.push_back(ptr);
        }
    }
    YT_VERIFY(erasureNumber == erasedIndices.size());

    TPartIndexList preparedErasedIndices = erasedIndices;
    preparedErasedIndices.push_back(-1);

    Y_ASSERT(blockPointers.size() == static_cast<size_t>(BlockCount));
    Y_ASSERT(parityPointers.size() == static_cast<size_t>(parityCount));

    int res = jerasure_schedule_decode_lazy(
        BlockCount,
        parityCount,
        WordSize,
        bitMatrix.Get(),
        preparedErasedIndices.data(),
        blockPointers.data(),
        parityPointers.data(),
        blockLength,
        sizeof(long),
        1);
    YT_VERIFY(res == 0);

    return std::vector<TBlobType>(repaired.begin(), repaired.end());
}

} // namespace NErasure
