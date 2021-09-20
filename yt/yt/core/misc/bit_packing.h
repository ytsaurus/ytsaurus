#pragma once

#include <yt/yt/core/misc/public.h>

#include <numeric>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCompressedVectorView
{
public:
    using TWord = ui64;
    static constexpr ui8 WordSize = sizeof(TWord) * 8;

    explicit TCompressedVectorView(const ui64* ptr = nullptr);

    size_t GetSize() const;

    size_t GetWidth() const;

    size_t GetSizeInWords() const;

    size_t GetSizeInBytes() const;

    TWord operator[] (size_t index) const;

    template <class T>
    void UnpackTo(T* output);

    TCompressedVectorView operator++();

private:
    const ui64* Ptr_;
    static constexpr int WidthBitsOffset = 56;
};

template <class T>
size_t UnpackBitVector(TCompressedVectorView view, std::vector<T>* container)
{
    container->resize(view.GetSize());
    view.UnpackTo(container->data());
    return view.GetSizeInWords();
}

template <class T>
size_t UnpackBitVector(const ui64* input, std::vector<T>* container)
{
    TCompressedVectorView view(input);
    return UnpackBitVector(view, container);
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class S, class T>
void UnpackValuesFallback(const S* input, T* output, T* outputEnd, ui8 width)
{
    constexpr ui8 WordSize = sizeof(S) * 8;
    YT_VERIFY(width != 0);

    ui8 offset = 0;
    S mask = MaskLowerBits(width);
    while (output != outputEnd) {
        S w = input[0] >> offset;

        if (offset + width > WordSize) {
            w |= input[1] << (WordSize - offset);
        }

        *output++ = static_cast<T>(w & mask);

        offset += width;
        input += offset / WordSize;
        offset &= WordSize - 1;
    }
}

template <class S, class T, ui8 Width, ui8 Remaining>
struct TUnroller
{
    static constexpr ui8 WordSize = sizeof(S) * 8;
    static constexpr ui8 UnrollFactor = WordSize / std::gcd(Width, WordSize);
    static constexpr S Mask = (1ULL << Width) - 1;

    static Y_FORCE_INLINE void Do(const S* input, T* output)
    {
        constexpr ui8 Index = UnrollFactor - Remaining;
        constexpr ui8 Offset = Index * Width % WordSize;

        auto value = input[Index * Width / WordSize] >> Offset;
        if constexpr (Offset + Width > WordSize) {
            value |= input[(Index + 1) * Width / WordSize] << (WordSize - Offset);
        }

        output[Index] = static_cast<T>(value & Mask);

        TUnroller<S, T, Width, Remaining - 1>::Do(input, output);
    }
};

template <class S, class T, ui8 Width>
struct TUnroller<S, T, Width, 0>
{
    static Y_FORCE_INLINE void Do(const S* /*input*/, T* /*output*/)
    { }
};

template <int Width, class S, class T>
void UnpackValuesUnrolled(const S* input, T* output, T* outputEnd)
{
    constexpr ui8 WordSize = sizeof(S) * 8;
    constexpr ui8 Gcd = std::gcd(Width, WordSize);
    constexpr ui8 UnrollFactor = WordSize / Gcd;
    constexpr ui8 UnrollBatch = Width / Gcd;

    while (output + UnrollFactor < outputEnd) {
        TUnroller<S, T, Width, UnrollFactor>::Do(input, output);
        output += UnrollFactor;
        input += UnrollBatch;
    }

    UnpackValuesFallback(input, output, outputEnd, Width);
}

template <class S, class T>
void UnpackValuesAligned(const S* input, T* output, T* outputEnd)
{
    while (output != outputEnd) {
        *output++ = *input++;
    }
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

inline TCompressedVectorView::TCompressedVectorView(const ui64* ptr)
    : Ptr_(ptr)
{ }

inline size_t TCompressedVectorView::GetSize() const
{
    return *Ptr_ & MaskLowerBits(WidthBitsOffset);
}

inline size_t TCompressedVectorView::GetWidth() const
{
    return *Ptr_ >> WidthBitsOffset;
}

inline size_t TCompressedVectorView::GetSizeInWords() const
{
    return 1 + (GetWidth() * GetSize() + WordSize - 1) / WordSize;
}

inline size_t TCompressedVectorView::GetSizeInBytes() const
{
    return GetSizeInWords() * sizeof(ui64);
}

inline TCompressedVectorView::TWord TCompressedVectorView::operator[] (size_t index) const
{
    auto width = GetWidth();

    if (width == 0) {
        return 0;
    }

    ui64 bitIndex = index * width;

    auto data = Ptr_ + 1 + bitIndex / WordSize;
    ui8 offset = bitIndex % WordSize;

    TWord w = data[0] >> offset;
    if (offset + width > WordSize) {
        w |= data[1] << (WordSize - offset);
    }

    return w & MaskLowerBits(width);
}

template <class T>
void TCompressedVectorView::UnpackTo(T* output)
{
    auto size = GetSize();
    auto width = GetWidth();
    auto input = Ptr_ + 1;

    YT_VERIFY(width <= 8 * sizeof(T));

    switch (width) {
        case  0: std::fill(output, output + size, 0); break;
        // Cast to const ui32* produces less instructions.
        #define UNROLLED(width, type) \
            case width: \
                NDetail::UnpackValuesUnrolled<width>(reinterpret_cast<const type*>(input), output, output + size); \
                break;
        #define ALIGNED(width, type) \
            case width: \
                NDetail::UnpackValuesAligned(reinterpret_cast<const type*>(input), output, output + size); \
                break;
        UNROLLED( 1, ui8)
        UNROLLED( 2, ui8)
        UNROLLED( 3, ui32)
        UNROLLED( 4, ui8)
        UNROLLED( 5, ui32)
        UNROLLED( 6, ui32)
        UNROLLED( 7, ui32)

        UNROLLED( 9, ui32)
        UNROLLED(10, ui32)
        UNROLLED(11, ui32)
        UNROLLED(12, ui32)
        UNROLLED(13, ui32)
        UNROLLED(14, ui32)
        UNROLLED(15, ui32)

        UNROLLED(17, ui32)
        UNROLLED(18, ui32)
        UNROLLED(19, ui32)
        UNROLLED(20, ui32)
        UNROLLED(21, ui32)
        UNROLLED(22, ui32)
        UNROLLED(23, ui32)
        UNROLLED(24, ui32)
        UNROLLED(25, ui32)
        UNROLLED(26, ui32)
        UNROLLED(27, ui32)
        UNROLLED(28, ui32)
        UNROLLED(29, ui32)
        UNROLLED(30, ui32)
        UNROLLED(31, ui32)

        ALIGNED( 8, ui8)
        ALIGNED(16, ui16)
        ALIGNED(32, ui32)
        ALIGNED(64, ui64)

        #undef UNROLLED
        #undef ALIGNED
        default:
            NDetail::UnpackValuesFallback(input, output, output + size, width);
            break;
    }
}

inline TCompressedVectorView TCompressedVectorView::operator++()
{
    Ptr_ += GetSizeInWords();
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

