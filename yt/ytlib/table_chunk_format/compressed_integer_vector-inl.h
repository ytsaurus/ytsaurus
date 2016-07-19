#pragma once

#if !defined(INCLUDE_COMPRESSED_INTEGER_VECTOR_INL_H)
#error "you should never include compressed_integer_vector-inl.h directly"
#endif  // INCLUDE_COMPRESSED_INTEGER_VECTOR_INL_H

#include <util/generic/bitops.h>

namespace NYT {
namespace NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

inline ui64 GetWidth(ui64 value)
{
    return (value == 0) ? 0 : MostSignificantBit(value) + 1;
}

inline size_t CompressedUnsignedVectorSizeInWords(ui64 maxValue, size_t count)
{
    // One word for the header.
    return 1 + ((GetWidth(maxValue) * count + 63ULL) >> 6ULL);
}

inline size_t CompressedUnsignedVectorSizeInBytes(ui64 maxValue, size_t count)
{
    static size_t wordSize = sizeof(ui64);
    return CompressedUnsignedVectorSizeInWords(maxValue, count) * wordSize;
}

template <class T>
typename std::enable_if<std::is_unsigned<T>::value, size_t>::type
CompressUnsignedVector(const TRange<T> values, ui64 maxValue, ui64* dst)
{
    ui64 width = GetWidth(maxValue);
    ui64 header = values.Size();

    // Check that most significant byte is empty.
    YCHECK((MaskLowerBits(8, 56) & header) == 0);
    header |= width << 56;

    // Save header.
    *dst = header;

    if (maxValue == 0) {
        // All values are zeros.
        return 1;
    }

    ui64* word = dst + 1;

    ui8 offset = 0;
    if (width < 64) {
        for (auto value : values) {
            // Cast to ui64 to do proper shifts.
            ui64 x = value;
            if (offset + width < 64) {
                *word |= (x << offset);
                offset += width;
            } else {
                *word |= (x << offset);
                offset = offset + width;
                offset &= 0x3F;
                ++word;
                x >>= width - offset;
                if (x > 0) {
                    // This is important not to overstep allocated boundaries.
                    *word |= x;
                }
            }
        }
    } else {
        // Custom path for 64-bits (especially useful since right shift on 64 bits does nothing).
        for (auto value : values) {
            *word = value;
            ++word;
        }
    }

    return (offset == 0 ? 0 : 1) + word - dst;
}


template <class T>
typename std::enable_if<std::is_unsigned<T>::value, TSharedRef>::type
CompressUnsignedVector(const TRange<T> values, ui64 maxValue)
{
    struct TCompressedUnsignedVectorTag {};

    size_t size = CompressedUnsignedVectorSizeInBytes(maxValue, values.Size());
    auto data = TSharedMutableRef::Allocate<TCompressedUnsignedVectorTag>(size);
    auto actualSize = CompressUnsignedVector(values, maxValue, reinterpret_cast<ui64*>(data.Begin()));
    YCHECK(size == actualSize * sizeof(ui64));

    return data;
};

////////////////////////////////////////////////////////////////////////////////

template <class T, bool Scan>
TCompressedUnsignedVectorReader<T, Scan>::TCompressedUnsignedVectorReader(const ui64* data)
    : Data_(data + 1)
    , Size_(*data & MaskLowerBits(56))
    , Width_(*data >> 56)
{
    if (Scan) {
        UnpackValues();
    }
}

template <class T, bool Scan>
TCompressedUnsignedVectorReader<T, Scan>::TCompressedUnsignedVectorReader()
    : Data_(nullptr)
    , Size_(0)
    , Width_(0)
{ }

template <class T, bool Scan>
inline T TCompressedUnsignedVectorReader<T, Scan>::operator[] (size_t index) const
{
    if (Scan) {
        return Values_[index];
    } else {
        return GetValue(index);
    }
}

template <class T, bool Scan>
inline size_t TCompressedUnsignedVectorReader<T, Scan>::GetSize() const
{
    return Size_;
}

template <class T, bool Scan>
inline size_t TCompressedUnsignedVectorReader<T, Scan>::GetByteSize() const
{
    if (Data_) {
        return (1 + ((Width_ * Size_ + 63ULL) >> 6ULL)) * sizeof(ui64);
    } else {
        return 0;
    }
}

template <class T, bool Scan>
T TCompressedUnsignedVectorReader<T, Scan>::GetValue(size_t index) const
{
    Y_ASSERT(index < Size_);

    if (Width_ == 0) {
        return 0;
    }

    ui64 bitIndex = index * Width_;
    const ui64* word = Data_ + (bitIndex >> 6);
    ui8 offset = bitIndex & 0x3F;

    ui64 w1 = (*word) >> offset;
    if (offset + Width_ > 64) {
        ++word;
        ui64 w2 = (*word & MaskLowerBits((offset + Width_) & 0x3F)) << (64 - offset);
        return static_cast<T>(w1 | w2);
    } else {
        return static_cast<T>(w1 & MaskLowerBits(Width_));
    }
}

template <class T, bool Scan>
void TCompressedUnsignedVectorReader<T, Scan>::UnpackValues()
{
    Values_.resize(Size_, 0);

    if (Width_ == 0) {
        return;
    }

    const ui64* word = Data_;
    ui8 offset = 0;

    for (size_t index = 0; index < Size_; ++index) {
        ui64 w1 = (*word) >> offset;
        if (offset + Width_ > 64) {
            ++word;
            ui64 w2 = (*word & MaskLowerBits((offset + Width_) & 0x3F)) << (64 - offset);
            Values_[index] = static_cast<T>(w1 | w2);
        } else {
            Values_[index] = static_cast<T>(w1 & MaskLowerBits(Width_));
        }

        offset = (offset + Width_) & 0x3F;
        if (offset == 0) {
            ++word;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableChunkFormat
} // namespace NYT
