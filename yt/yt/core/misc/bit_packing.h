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

    TCompressedVectorView() = default;

    explicit TCompressedVectorView(const ui64* ptr);

    TCompressedVectorView(const ui64* ptr, ui32 size, ui8 width);

    size_t GetSize() const;

    size_t GetWidth() const;

    size_t GetSizeInWords() const;

    size_t GetSizeInBytes() const;

    TWord operator[] (size_t index) const;

    template <class T>
    void UnpackTo(T* output);

    template <class T>
    void UnpackTo(T* output, ui32 start, ui32 end);

private:
    static constexpr int WidthBitsOffset = 56;

    const ui64* Ptr_ = nullptr;
    ui32 Size_ = 0;
    ui8 Width_ = 0;
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

inline TCompressedVectorView::TCompressedVectorView(const ui64* ptr)
    : Ptr_(ptr)
    , Size_(*ptr & MaskLowerBits(WidthBitsOffset))
    , Width_(*ptr >> WidthBitsOffset)
{
    if (Width_ != 0) {
        ++Ptr_;
    }
}

inline TCompressedVectorView::TCompressedVectorView(const ui64* ptr, ui32 size, ui8 width)
    // Considering width here helps to avoid extra branch when extracting element.
    : Ptr_(ptr + (width != 0))
    , Size_(size)
    , Width_(width)
{
    YT_ASSERT((*ptr & MaskLowerBits(WidthBitsOffset)) == size);
    YT_ASSERT((*ptr >> WidthBitsOffset) == width);
}

inline size_t TCompressedVectorView::GetSize() const
{
    return Size_;
}

inline size_t TCompressedVectorView::GetWidth() const
{
    return Width_;
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
    YT_ASSERT(index < GetSize());

    auto width = GetWidth();

    ui64 bitIndex = index * width;

    auto* data = Ptr_ + bitIndex / WordSize;
    ui8 offset = bitIndex % WordSize;

    TWord w = data[0] >> offset;
    if (offset + width > WordSize) {
        w |= data[1] << (WordSize - offset);
    }

    return w & MaskLowerBits(width);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

