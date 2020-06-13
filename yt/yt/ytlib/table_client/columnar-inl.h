#pragma once
#ifndef COLUMNAR_INL_H_
#error "Direct inclusion of this file is not allowed, include columnar.h"
// For the sake of sane code completion.
#include "columnar.h"
#endif

#include <yt/core/misc/zigzag.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

inline i64 GetBitmapByteSize(i64 bitCount)
{
    return (bitCount + 7) / 8;
}

inline i64 DecodeStringOffset(
    TRange<ui32> offsets,
    ui32 avgLength,
    ui32 index)
{
    YT_ASSERT(index >= 0 && index <= offsets.Size());
    return index == 0
        ? 0
        : static_cast<ui32>(avgLength * index + ZigZagDecode64(offsets[index - 1]));
}

template <class S, class T, class F>
void DecodeRleVector(
    TRange<S> values,
    TRange<ui64> rleIndexes,
    i64 startIndex,
    i64 endIndex,
    F valueDecoder,
    TMutableRange<T> dst)
{
    YT_VERIFY(startIndex >= 0 && startIndex <= endIndex);
    YT_VERIFY(endIndex - startIndex == dst.Size());
    YT_VERIFY(values.Size() == rleIndexes.Size());
    YT_VERIFY(rleIndexes[0] == 0);

    auto startRleIndex = TranslateRleStartIndex(rleIndexes, startIndex);
    auto* currentOutput = dst.Begin();
    const auto* currentInput = values.Begin() + startRleIndex;
    auto currentIndex = startIndex;
    auto currentRleIndex = startRleIndex;
    T currentValue;
    i64 thresholdIndex = -1;
    while (currentIndex < endIndex) {
        if (currentIndex >= thresholdIndex) {
            ++currentRleIndex;
            thresholdIndex = currentRleIndex < rleIndexes.Size() ? static_cast<i64>(rleIndexes[currentRleIndex]) : Max<i64>();
            currentValue = valueDecoder(*currentInput++);
        }
        *currentOutput++ = currentValue;
        ++currentIndex;
    }
}

template <class T, bool ZigZagEncoded>
inline T DecodeIntegerValueImpl(
    ui64 value,
    ui64 baseValue)
{
    value += baseValue;
    if constexpr(ZigZagEncoded) {
        value = static_cast<ui64>(ZigZagDecode64(value));
    }
    return static_cast<T>(value);
}

template <class T>
T DecodeIntegerValue(
    ui64 value,
    ui64 baseValue,
    bool zigZagEncoded)
{
    return zigZagEncoded
        ? DecodeIntegerValueImpl<T, true>(value, baseValue)
        : DecodeIntegerValueImpl<T, false>(value, baseValue);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
