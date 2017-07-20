#pragma once

#include "blob.h"
#include "ref.h"
#include "small_vector.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TChunkType>
struct TBitmapTraits
{
    // In this case compiler can replace divisions and modulos with shifts.
    static_assert(
        !(sizeof(TChunkType) & (sizeof(TChunkType) - 1)),
        "sizeof(TChunkType) must be a power of 2.");
    static constexpr size_t Bytes = sizeof(TChunkType);
    static constexpr size_t Bits = Bytes * 8;

    static constexpr size_t GetChunkIndex(size_t bitIndex)
    {
        return bitIndex / Bits;
    }

    static constexpr TChunkType GetChunkMask(size_t bitIndex, bool value)
    {
        return (value ? TChunkType(1) : TChunkType(0)) << (bitIndex % Bits);
    }

    static constexpr size_t GetChunkCapacity(size_t bitCapacity)
    {
        return (bitCapacity + Bits - 1) / Bits;
    }

    static constexpr size_t GetByteCapacity(size_t bitCapacity)
    {
        return GetChunkCapacity(bitCapacity) * Bytes;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TChunkType, int DefaultChunkCount = 1>
class TAppendOnlyBitmap
{
    using TTraits = TBitmapTraits<TChunkType>;

public:
    explicit TAppendOnlyBitmap(size_t bitCapacity = 0)
    {
        if (bitCapacity) {
            Chunks_.reserve(TTraits::GetChunkCapacity(bitCapacity));
        }
    }

    void Append(bool value)
    {
        if (Chunks_.size() * sizeof(TChunkType) * 8 == BitSize_) {
            Chunks_.push_back(TChunkType());
        }
        Chunks_.back() |= TTraits::GetChunkMask(BitSize_, value);
        ++BitSize_;
    }

    bool operator[](size_t bitIndex) const
    {
        Y_ASSERT(bitIndex < BitSize_);
        const auto chunkIndex = TTraits::GetChunkIndex(bitIndex);
        const auto chunkMask = TTraits::GetChunkMask(bitIndex, true);
        return (Chunks_[chunkIndex] & chunkMask) != 0;
    }

    size_t GetBitSize() const
    {
        return BitSize_;
    }

    template <class TTag>
    TSharedRef Flush()
    {
        auto blob = TBlob(TTag(), Chunks_.data(), Size());
        return TSharedRef::FromBlob(std::move(blob));
    }

    const TChunkType* Data() const
    {
        return Chunks_.data();
    }

    size_t Size() const
    {
        return Chunks_.size() * sizeof(TChunkType);
    }

private:
    SmallVector<TChunkType, DefaultChunkCount> Chunks_;
    size_t BitSize_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <class TChunkType>
class TReadOnlyBitmap
{
    using TTraits = TBitmapTraits<TChunkType>;

public:
    TReadOnlyBitmap() = default;

    TReadOnlyBitmap(const TChunkType* chunks, size_t bitSize)
        : Chunks_(chunks)
        , BitSize_(bitSize)
    { }

    void Reset(const TChunkType* chunks, size_t bitSize)
    {
        YCHECK(chunks);
        Chunks_ = chunks;
        BitSize_ = bitSize;
    }

    bool operator[](size_t bitIndex) const
    {
        Y_ASSERT(bitIndex < BitSize_);
        const auto chunkIndex = TTraits::GetChunkIndex(bitIndex);
        const auto chunkMask = TTraits::GetChunkMask(bitIndex, true);
        return (Chunks_[chunkIndex] & chunkMask) != 0;
    }

    size_t GetByteSize() const
    {
        return TTraits::GetByteCapacity(BitSize_);
    }

    void Prefetch(size_t bitIndex)
    {
        Y_ASSERT(bitIndex < BitSize_);
        const auto chunkIndex = TTraits::GetChunkIndex(bitIndex);
        __builtin_prefetch(&Chunks_[chunkIndex], 0); // read-only prefetch
    }

private:
    const TChunkType* Chunks_ = nullptr;
    size_t BitSize_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
