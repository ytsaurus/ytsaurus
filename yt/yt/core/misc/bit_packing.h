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

    template <class T>
    void UnpackTo(T* output, ui32 start, ui32 end);

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
    YT_ASSERT(index < GetSize());

    auto width = GetWidth();

    if (Y_UNLIKELY(width == 0)) {
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

inline TCompressedVectorView TCompressedVectorView::operator++()
{
    Ptr_ += GetSizeInWords();
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

