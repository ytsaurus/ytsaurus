#pragma once

// TODO(lukyan): Rename this file to column_base.h

#include <yt/yt/library/numeric/algorithm_helpers.h>

#include <library/cpp/yt/memory/ref.h>

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
    TColumnBase() = default;

    // Copy constructor cannot be inherited. Pass object by pointer.
    explicit TColumnBase(const TColumnBase* columnInfo);

    TColumnBase(const TBlockRef* blockRef, ui16 indexInGroup);

    bool IsNull() const;

    TRef GetBlock() const;

    template <class TMeta>
    TRange<TMeta> GetSegmentMetas() const;

    template <class TMeta>
    const TMeta* SkipToSegment(ui32 rowIndex) const;

private:
    const TBlockRef* BlockRef_;
    ui16 IndexInGroup_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient

#define BLOCK_REF_INL_H_
#include "block_ref-inl.h"
#undef BLOCK_REF_INL_H_
