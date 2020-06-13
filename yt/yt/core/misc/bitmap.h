#pragma once

#include "blob.h"
#include "ref.h"
#include "small_vector.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TChunk>
struct TBitmapTraits
{
    // In this case compiler can replace divisions and modulos with shifts.
    static_assert(
        !(sizeof(TChunk) & (sizeof(TChunk) - 1)),
        "sizeof(TChunk) must be a power of 2.");
    static constexpr size_t Bytes = sizeof(TChunk);
    static constexpr size_t Bits = Bytes * 8;

    static constexpr size_t GetChunkIndex(size_t bitIndex)
    {
        return bitIndex / Bits;
    }

    static constexpr TChunk GetChunkMask(size_t bitIndex, bool value)
    {
        return (value ? TChunk(1) : TChunk(0)) << (bitIndex % Bits);
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

template <class TChunk, int DefaultChunkCount = 1>
class TAppendOnlyBitmap
{
    using TTraits = TBitmapTraits<TChunk>;

public:
    explicit TAppendOnlyBitmap(size_t bitCapacity = 0)
    {
        if (bitCapacity) {
            Chunks_.reserve(TTraits::GetChunkCapacity(bitCapacity));
        }
    }

    void Append(bool value)
    {
        if (Chunks_.size() * sizeof(TChunk) * 8 == BitSize_) {
            Chunks_.push_back(TChunk());
        }
        Chunks_.back() |= TTraits::GetChunkMask(BitSize_, value);
        ++BitSize_;
    }

    bool operator[](size_t bitIndex) const
    {
        YT_ASSERT(bitIndex < BitSize_);
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

    const TChunk* Data() const
    {
        return Chunks_.data();
    }

    size_t Size() const
    {
        return Chunks_.size() * sizeof(TChunk);
    }

private:
    SmallVector<TChunk, DefaultChunkCount> Chunks_;
    size_t BitSize_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <class TChunk>
class TReadOnlyBitmap
{
private:
    using TTraits = TBitmapTraits<TChunk>;

public:
    TReadOnlyBitmap() = default;

    TReadOnlyBitmap(const TChunk* chunks, size_t bitSize)
        : Chunks_(chunks)
        , BitSize_(bitSize)
    { }

    void Reset(const TChunk* chunks, size_t bitSize)
    {
        YT_VERIFY(chunks);
        Chunks_ = chunks;
        BitSize_ = bitSize;
    }

    bool operator[](size_t bitIndex) const
    {
        YT_ASSERT(bitIndex < BitSize_);
        const auto chunkIndex = TTraits::GetChunkIndex(bitIndex);
        const auto chunkMask = TTraits::GetChunkMask(bitIndex, true);
        return (Chunks_[chunkIndex] & chunkMask) != 0;
    }

    size_t GetBitSize() const
    {
        return BitSize_;
    }

    size_t GetByteSize() const
    {
        return TTraits::GetByteCapacity(BitSize_);
    }

    TRange<TChunk> GetData() const
    {
        return MakeRange(Chunks_, TTraits::GetChunkCapacity(BitSize_));
    }

    void Prefetch(size_t bitIndex)
    {
        YT_ASSERT(bitIndex < BitSize_);
        const auto chunkIndex = TTraits::GetChunkIndex(bitIndex);
        __builtin_prefetch(&Chunks_[chunkIndex], 0); // read-only prefetch
    }

private:
    const TChunk* Chunks_ = nullptr;
    size_t BitSize_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
