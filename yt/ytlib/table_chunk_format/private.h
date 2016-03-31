#pragma once

#include <yt/core/misc/common.h>

#include <yt/core/misc/small_vector.h>

namespace NYT {
namespace NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EVersionedIntegerSegmentType,
    ((DictionarySparse)   (0))
    ((DictionaryDense)    (1))
    ((DirectSparse)       (2))
    ((DirectDense)        (3))
);

DEFINE_ENUM(EVersionedStringSegmentType,
    ((DictionarySparse)   (0))
    ((DictionaryDense)    (1))
    ((DirectSparse)       (2))
    ((DirectDense)        (3))
);

DEFINE_ENUM(EUnversionedIntegerSegmentType,
    ((DictionaryRle)   (0))
    ((DictionaryDense) (1))
    ((DirectRle)       (2))
    ((DirectDense)     (3))
);

DEFINE_ENUM(EUnversionedStringSegmentType,
    ((DictionaryRle)   (0))
    ((DictionaryDense) (1))
    ((DirectRle)       (2))
    ((DirectDense)     (3))
);

////////////////////////////////////////////////////////////////////////////////

struct TSegment
{
    NProto::TSegmentMeta Meta;
    std::vector<TSharedRef> Data;
};

////////////////////////////////////////////////////////////////////////////////

struct TSegmentWriterTag
{};

////////////////////////////////////////////////////////////////////////////////

// ToDo(psushin) : unite with TSegment.
struct TSegmentInfo
{
    NProto::TSegmentMeta SegmentMeta;
    std::vector<TSharedRef> Data;
    bool Dense;
};

////////////////////////////////////////////////////////////////////////////////

using TTimestampIndex = ui32;
using TTimestampIndexes = SmallVector<TTimestampIndex, 10>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableChunkFormat
} // namespace NYT
