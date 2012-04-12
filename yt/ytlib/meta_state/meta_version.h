#pragma once

#include "public.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

struct TMetaVersion
{
    i32 SegmentId;
    i32 RecordCount;

    TMetaVersion();
    TMetaVersion(i32 segmentId, i32 recordCount);

    bool operator < (const TMetaVersion& other) const;
    bool operator == (const TMetaVersion& other) const;
    bool operator != (const TMetaVersion& other) const;
    bool operator > (const TMetaVersion& other) const;
    bool operator <= (const TMetaVersion& other) const;
    bool operator >= (const TMetaVersion& other) const;

    Stroka ToString() const
    {
        return Sprintf("%d:%d", SegmentId, RecordCount);
    }
};

inline TMetaVersion::TMetaVersion()
    : SegmentId(0)
    , RecordCount(0)
{  }

inline TMetaVersion::TMetaVersion(i32 segmentId, i32 recordCount)
    : SegmentId(segmentId)
    , RecordCount(recordCount)
{  }

inline bool TMetaVersion::operator < (const TMetaVersion& other) const
{
    return
        SegmentId < other.SegmentId ||
        (SegmentId == other.SegmentId && RecordCount < other.RecordCount);
}

inline bool TMetaVersion::operator == (const TMetaVersion& other) const
{
    return SegmentId == other.SegmentId && RecordCount == other.RecordCount;
}

inline bool TMetaVersion::operator != (const TMetaVersion& other) const
{
    return !(*this == other);
}

inline bool TMetaVersion::operator > (const TMetaVersion& other) const
{
    return !(*this <= other);
}

inline bool TMetaVersion::operator <= (const TMetaVersion& other) const
{
    return *this < other || *this == other;
}

inline bool TMetaVersion::operator >= (const TMetaVersion& other) const
{
    return !(*this < other);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
