#pragma once 

#include <yt/core/misc/range.h>

#include <yt/core/misc/ref.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

template <class T>
typename std::enable_if<std::is_unsigned<T>::value, TSharedRef>::type
CompressUnsignedVector(TRange<T> values, ui64 maxValue);

/*!
 *  \note Memory allocated under #dst must be initialized with zeroes.
 */
template <class T>
typename std::enable_if<std::is_unsigned<T>::value, size_t>::type
CompressUnsignedVector(TRange<T> values, ui64 maxValue, ui64* dst);

////////////////////////////////////////////////////////////////////////////////

template <class T, bool Scan = true>
class TCompressedUnsignedVectorReader
{
public:
    explicit TCompressedUnsignedVectorReader(const ui64* data);

    TCompressedUnsignedVectorReader();

    T operator[] (size_t index) const;

    //! Number of elements in the vector.
    size_t GetSize() const;

    //! Number of bytes occupied by the vector.
    size_t GetByteSize() const;

private:
    const ui64* Data_;
    size_t Size_;
    ui8 Width_;

    const T* Values_;
    std::unique_ptr<T[]> ValuesHolder_;

    T GetValue(size_t index) const;
    void UnpackValues();
    template <int Width>
    void UnpackValuesUnrolled();
    template <class S>
    void UnpackValuesAligned();
    void UnpackValuesFallback();
    
};

////////////////////////////////////////////////////////////////////////////////

void PrepareDiffFromExpected(std::vector<ui32>* values, ui32* expected, ui32* maxDiff);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat

#define INCLUDE_COMPRESSED_INTEGER_VECTOR_INL_H
#include "compressed_integer_vector-inl.h"
#undef INCLUDE_COMPRESSED_INTEGER_VECTOR_INL_H
