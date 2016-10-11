#pragma once

#include "blob.h"
#include "ref.h"
#include "small_vector.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TChunkType>
TChunkType GetChunkMask(size_t bitIndex, bool value)
{
    static_assert(
        !(sizeof(TChunkType) & (sizeof(TChunkType) - 1)),
        "sizeof(TChunkType) must be a power of 2.");
    static constexpr size_t ChunkBytes = sizeof(TChunkType);
    static constexpr size_t ChunkBits = ChunkBytes * 8;
    auto mask = (value ? TChunkType(1) : TChunkType(0)) << (bitIndex % ChunkBits);
    return mask;
    // NB: Self-check to avoid nasty problems. See for example YT-5161.
    // auto y = (TChunkType(1)) << (bitIndex % ChunkBits);
    // YCHECK(x & ~y == 0);
}

////////////////////////////////////////////////////////////////////////////////

template <class TChunkType, int DefaultChunkCount = 1>
class TAppendOnlyBitmap
{
public:
    explicit TAppendOnlyBitmap(int bitCapacity = 0)
    {
        YCHECK(bitCapacity >= 0);
        if (bitCapacity) {
            Data_.reserve((bitCapacity - 1) / sizeof(TChunkType) / 8 + 1);
        }
    }

    void Append(bool value)
    {
        if (Data_.size() * sizeof(TChunkType) * 8 == BitSize_) {
            Data_.push_back(TChunkType());
        }

        Data_.back() |= GetChunkMask<TChunkType>(BitSize_, value);
        ++BitSize_;
    }

    bool operator[] (i64 index) const
    {
        Y_ASSERT(index < BitSize_);
        int dataIndex = index / (sizeof(TChunkType) * 8);
        return static_cast<bool>(Data_[dataIndex] & GetChunkMask<TChunkType>(index, true));
    }

    i64 GetBitSize() const
    {
        return BitSize_;
    }

    template <class TTag>
    TSharedRef Flush()
    {
        auto blob = TBlob(TTag(), Data_.data(), Size());
        return TSharedRef::FromBlob(std::move(blob));
    }

    const TChunkType* Data() const
    {
        return Data_.data();
    }

    int Size() const
    {
        return Data_.size() * sizeof(TChunkType);
    }

private:
    i64 BitSize_ = 0;
    SmallVector<TChunkType, DefaultChunkCount> Data_;

};

////////////////////////////////////////////////////////////////////////////////

template <class TChunkType>
class TReadOnlyBitmap
{
    // In this case compiler can replace divisions and modulos with shifts.
    static_assert(
        !(sizeof(TChunkType) & (sizeof(TChunkType) - 1)),
        "sizeof(TChunkType) must be a power of 2.");
    static constexpr size_t ChunkBytes = sizeof(TChunkType);
    static constexpr size_t ChunkBits = ChunkBytes * 8;

public:
    TReadOnlyBitmap() = default;

    TReadOnlyBitmap(const TChunkType* chunks, size_t bitSize)
        : Chunks_(chunks)
        , BitSize_(bitSize)
    { }

    void Reset(const TChunkType* chunks, size_t bitSize)
    {
        YCHECK(chunks);
        YCHECK(bitSize >= 0);
        Chunks_ = chunks;
        BitSize_ = bitSize;
    }

    bool operator[] (size_t bitIndex) const
    {
        Y_ASSERT(bitIndex < BitSize_);
        auto chunkIndex = bitIndex / ChunkBits;
        auto chunkOffset = bitIndex % ChunkBits;
        TChunkType value = Chunks_[chunkIndex];
        TChunkType mask = TChunkType(1) << chunkOffset;
        return static_cast<bool>(value & mask);
    }

    int GetByteSize() const
    {
        return ((BitSize_ + ChunkBits - 1) / ChunkBits) * ChunkBytes;
    }

    void Prefetch(size_t bitIndex)
    {
        Y_ASSERT(bitIndex < BitSize_);
        auto chunkIndex = bitIndex / ChunkBits;
        __builtin_prefetch(&Chunks_[chunkIndex], 0); // read-only prefetch
    }

private:
    const TChunkType* Chunks_ = nullptr;
    size_t BitSize_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
