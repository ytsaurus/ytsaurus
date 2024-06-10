#include "bitset.h"


namespace {

// Need to align to start of bitmap bucket, it is (sizeof(ui64) * 8).
constexpr ui32 AlignMask = ~(static_cast<ui32>(sizeof(ui64) * 8) - 1);

inline void MergeInplace(
    TDynBitMap& (TDynBitMap::* method)(const TDynBitMap&),
    TDynBitMap& bitmap,
    i32& offset,
    const TDynBitMap& otherBitmap,
    i32 otherOffset)
{
    if (offset == otherOffset) {
        (bitmap.*method)(otherBitmap);
    } else if (offset < otherOffset) {
        (bitmap.*method)(otherBitmap << static_cast<size_t>(otherOffset - offset));
    } else {
        bitmap.LShift(static_cast<size_t>(offset - otherOffset));
        offset = otherOffset;
        (bitmap.*method)(otherBitmap);
    }
}

} // namespace

std::pair<TBitSet<i32>::iterator, bool> TBitSet<i32>::emplace(i32 key)
{
    auto it = find(key);
    if (it == end()) {
        if (key < Offset_) {
            const i32 newOffset = key & AlignMask;
            if (Offset_ != Max<i32>()) {
                // Bitset is not empty, empty is expensive method, so use offset as marker.
                BitMap_.LShift(static_cast<ui32>(Offset_ - newOffset));
            }
            Offset_ = newOffset;
        }
        const size_t realIndex = static_cast<ui32>(key - Offset_);
        BitMap_.Set(realIndex);
        return std::pair<TBitSet::iterator, bool>(iterator(this, realIndex), true);
    }
    return std::pair(it, false);
}

TBitSet<i32>& TBitSet<i32>::operator|=(const TBitSet<i32>& other)
{
    MergeInplace(&TDynBitMap::Or, BitMap_, Offset_, other.BitMap_, other.Offset_);
    return *this;
}

TBitSet<i32>& TBitSet<i32>::operator&=(const TBitSet<i32>& other)
{
    MergeInplace(&TDynBitMap::And, BitMap_, Offset_, other.BitMap_, other.Offset_);
    return *this;
}

TBitSet<i32>& TBitSet<i32>::operator^=(const TBitSet<i32>& other)
{
    MergeInplace(&TDynBitMap::Xor, BitMap_, Offset_, other.BitMap_, other.Offset_);
    return *this;
}
