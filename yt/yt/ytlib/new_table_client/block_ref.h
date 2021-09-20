#pragma once

#include <yt/yt/core/misc/ref.h>
#include <yt/yt/core/misc/algorithm_helpers.h>

namespace NYT::NNewTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TBlockRef
{
    // Blob value data is stored in blocks without capturing in TRowBuffer.
    // Therefore block must not change from the current call of ReadRows till the next one.

    TSharedRef Block;
    TRef BlockSegmentsMeta;
};

struct TColumnBase
{
public:
    explicit TColumnBase(const TColumnBase* columnInfo);

    TColumnBase(const TBlockRef* blockRef, ui16 indexInGroup);

    bool IsNull() const;

    TRef GetBlock() const;

    template <class TMeta>
    TRange<TMeta> GetSegmentMetas() const;

    template <class TMeta>
    const TMeta* SkipToSegment(ui32 rowIndex) const;

private:
    const TBlockRef* const BlockRef_;
    const ui16 IndexInGroup_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient

#define BLOCK_REF_INL_H_
#include "block_ref-inl.h"
#undef BLOCK_REF_INL_H_
