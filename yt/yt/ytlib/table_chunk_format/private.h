#pragma once

#include <yt/yt/core/misc/common.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

namespace NYT::NTableChunkFormat {

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
using TTimestampIndexes = TCompactVector<TTimestampIndex, 10>;

static constexpr ui32 SlimVersionedValueTagNull = 0x0001;
static constexpr ui32 SlimVersionedValueTagAggregate = 0x0002;
static constexpr ui32 SlimVersionedValueTagDictionaryPayload = 0x0004;
static constexpr int SlimVersionedIdValueTagShift = 3;
static constexpr ui32 SlimVersionedDictionaryTagEos = 0x0001;
static constexpr int SlimVersionedDictionaryTagIndexShift = 1;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
