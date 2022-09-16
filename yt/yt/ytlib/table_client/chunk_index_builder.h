#pragma once

#include "public.h"

#include <yt/yt/core/misc/range.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IChunkIndexBuilder
    : public TRefCounted
{
    virtual i64 GetSectorAlignmentSize() const = 0;

    virtual bool IsGroupReorderingEnabled() const = 0;

    virtual int GetGroupCount() const = 0;
    virtual int GetGroupIdFromValueId(int valueId) const = 0;

    virtual void ProcessRow(
        TVersionedRow row,
        int blockIndex,
        i64 rowOffset,
        i64 rowLength,
        TRange<int> groupOffsets,
        TRange<int> groupIndexes) = 0;

    virtual TSharedRef BuildIndex() = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkIndexBuilder)

////////////////////////////////////////////////////////////////////////////////

IChunkIndexBuilderPtr CreateChunkIndexBuilder();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
