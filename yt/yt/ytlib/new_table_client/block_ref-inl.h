#ifndef BLOCK_REF_INL_H_
#error "Direct inclusion of this file is not allowed, include block_ref.h"
// For the sake of sane code completion.
#include "block_ref.h"
#endif
#undef BLOCK_REF_INL_H_

namespace NYT::NNewTableClient {

////////////////////////////////////////////////////////////////////////////////

inline TColumnBase::TColumnBase(const TColumnBase* columnInfo)
    : TColumnBase(*columnInfo)
{ }

inline TColumnBase::TColumnBase(const TBlockRef* blockRef, ui16 indexInGroup)
    : BlockRef_(blockRef)
    , IndexInGroup_(indexInGroup)
{ }

inline bool TColumnBase::IsNull() const
{
    return !BlockRef_;
}

inline TRef TColumnBase::GetBlock() const
{
    return IsNull() ? TRef() : BlockRef_->Block;
}

template <class TMeta>
TRange<TMeta> TColumnBase::GetSegmentMetas() const
{
    if (!BlockRef_->BlockSegmentsMeta) {
        return {};
    }

    auto blockMeta = BlockRef_->BlockSegmentsMeta.Begin();
    auto* offsets = reinterpret_cast<const ui32*>(blockMeta);

    auto result = MakeRange(
        reinterpret_cast<const TMeta*>(blockMeta + offsets[IndexInGroup_]),
        reinterpret_cast<const TMeta*>(blockMeta + offsets[IndexInGroup_ + 1]));

    return result;
}

template <class TMeta>
const TMeta* TColumnBase::SkipToSegment(ui32 rowIndex) const
{
    auto segmentMetas = GetSegmentMetas<TMeta>();

    auto segmentIt = BinarySearch(
        segmentMetas.begin(),
        segmentMetas.end(),
        [&] (auto segmentMetaIt) {
            return segmentMetaIt->ChunkRowCount <= rowIndex;
        });

    // Iterator can be out of range when reading sentinel rows after chunk row limit.
    return segmentIt != segmentMetas.end() ? segmentIt : nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
