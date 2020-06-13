#pragma once

#include "public.h"

#include <yt/core/misc/range.h>
#include <yt/core/misc/ref.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Returns the minimum number of bytes needed to store a bitmap with #bitCount bits. 
i64 GetBitmapByteSize(i64 bitCount);

//! Builds validity bitmap from #dictionaryIndexes by replacing each zero with
//! 0-bit and each non-zero with 1-bit.
void BuildValidityBitmapFromDictionaryIndexesWithZeroNull(
    TRange<ui32> dictionaryIndexes,
    TMutableRef dst);

//! Same as #BuildValidityBitmapFromDictionaryIndexesWithZeroNull but for
//! for RLE-encoded #dictionaryIndexes.
void BuildValidityBitmapFromRleDictionaryIndexesWithZeroNull(
    TRange<ui32> dictionaryIndexes,
    TRange<ui64> rleIndexes,
    i64 startIndex,
    i64 endIndex,
    TMutableRef dst);

//! Copies dictionary indexes from #dictionaryIndexes to #dst subtracting one.
//! Zeroes are replaced by unspecified values.
//! The size of #dst must be exactly |endIndex - startIndex|.
void BuildDictionaryIndexesFromDictionaryIndexesWithZeroNull(
    TRange<ui32> dictionaryIndexes,
    TMutableRange<ui32> dst);

//! Same as #BuildDictionaryIndexesFromDictionaryIndexesWithZeroNull but
//! for RLE-encoded #dictionaryIndexes.
void BuildDictionaryIndexesFromRleDictionaryIndexesWithZeroNull(
    TRange<ui32> dictionaryIndexes,
    TRange<ui64> rleIndexes,
    i64 startIndex,
    i64 endIndex,
    TMutableRange<ui32> dst);

//! Writes a "iota"-like sequence (0, 1, 2, ...) to #dst indicating
//! RLE groups in accordance to #rleIndexes.
//! The size of #dst must be exactly |endIndex - startIndex|.
void BuildIotaDictionaryIndexesFromRleIndexes(
    TRange<ui64> rleIndexes,
    i64 startIndex,
    i64 endIndex,
    TMutableRange<ui32> dst);

//! Counts the number of #indexes equal to zero.
i64 CountNullsInDictionaryIndexesWithZeroNull(TRange<ui32> indexes);

//! Same as #CountNullsInDictionaryIndexesWithZeroNull but
//! for RLE-encoded #dictionaryIndexes.
i64 CountNullsInRleDictionaryIndexesWithZeroNull(
    TRange<ui32> dictionaryIndexes,
    TRange<ui64> rleIndexes,
    i64 startIndex,
    i64 endIndex);

//! Counts the number of 1-bits in range [#startIndex, #endIndex) in #bitmap.
i64 CountOnesInBitmap(
    TRef bitmap,
    i64 startIndex,
    i64 endIndex);

//! Same as #CountOnesInBitmap but for RLE-encoded #bitmap.
i64 CountOnesInRleBitmap(
    TRef bitmap,
    TRange<ui64> rleIndexes,
    i64 startIndex,
    i64 endIndex);

//! Copies bits in range [#startIndex, #endIndex) from #bitmap to #dst.
//! #bitmap must be 8-byte aligned and its trailing qword must be readable.
//! The byte size of #dst be be enough to store |endIndex - startIndex| bits.
void CopyBitmapRange(
    TRef bitmap,
    i64 startIndex,
    i64 endIndex,
    TMutableRef dst);

//! Same as #CopyBitmapRange but inverts the bits.
void CopyBitmapRangeNegated(
    TRef bitmap,
    i64 startIndex,
    i64 endIndex,
    TMutableRef dst);

//! Decodes RLE-encoded #bitmap and inverts the bits.
//! The byte size of #dst be be enough to store |endIndex - startIndex| bits.
void BuildValidityBitmapFromRleNullBitmap(
    TRef bitmap,
    TRange<ui64> rleIndexes,
    i64 startIndex,
    i64 endIndex,
    TMutableRef dst);

//! Decodes a vector of integers from YT chunk representation into
//! a raw sequence. The byte size of #dst must exactly match
//! the byte size of #type times the number of elements in #values. 
//! \seealso DecodeIntegerValue
void DecodeIntegerVector(
    TRange<ui64> values,
    ESimpleLogicalValueType type,
    ui64 baseValue,
    bool zigzagEncoded,
    TMutableRef dst);

//! Decodes the starting offset of the #index-th string.
//! #index must be in range [0, N], where N is the size of #offsets.
i64 DecodeStringOffset(
    TRange<ui32> offsets,
    i64 index,
    ui32 avgLength);

//! Decodes the starting offsets of strings in [#startIndex, #endIndex)
//! range given by #offsets. The resulting array contains 32-bit offsets and
//! must be of size |endIndex - startIndex + 1| (the last element will indicate the
//! offset where the last string ends).
void DecodeStringOffsets(
    TRange<ui32> offsets,
    ui32 avgLength,
    i64 startIndex,
    i64 endIndex,
    TMutableRange<ui32> dst);

//! Given #rleIndexes, translates #index in actual rowspace to 
//! to the index in RLE-compressed rowspace.
i64 TranslateRleIndex(
    TRange<ui64> rleIndexes,
    i64 index); 

//! A synonym for TranslateRleIndex.
i64 TranslateRleStartIndex(
    TRange<ui64> rleIndexes,
    i64 index); 

//! Regards #index as the end (non-inclusive) index.
//! Equivalent to |TranslateRleIndex(index - 1) + 1| (unless index = 0).
i64 TranslateRleEndIndex(
    TRange<ui64> rleIndexes,
    i64 index); 

//! Decodes RLE-encoded in range [#startIndex, #endIndex).
//! Results pass #valueDecoder before being written to #dst.
//! The size of #dst must be |endIndex - startIndex|. 
template <class S, class T, class F>
void DecodeRleVector(
    TRange<S> values,
    TRange<ui64> rleIndexes,
    i64 startIndex,
    i64 endIndex,
    F valueDecoder,
    TMutableRange<T> dst);

//! Decodes a single integer from YT chunk representation.
template <class T>
T DecodeIntegerValue(
    ui64 value,
    ui64 baseValue,
    bool zigZagEncoded);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

#define COLUMNAR_INL_H_
#include "columnar-inl.h"
#undef COLUMNAR_INL_H_
