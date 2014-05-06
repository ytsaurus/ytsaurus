#pragma once

#include "public.h"

#include <util/generic/typetraits.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TVersion
{
    i32 SegmentId;
    i32 RecordId;

    TVersion();
    TVersion(i32 segmentId, i32 recordId);

    bool operator < (TVersion other) const;
    bool operator == (TVersion other) const;
    bool operator != (TVersion other) const;
    bool operator > (TVersion other) const;
    bool operator <= (TVersion other) const;
    bool operator >= (TVersion other) const;

    i64 ToRevision() const;
    static TVersion FromRevision(i64 revision);

    TVersion Advance() const;
    TVersion Rotate() const;

};

Stroka ToString(TVersion version);

////////////////////////////////////////////////////////////////////////////////

inline TVersion::TVersion()
    : SegmentId(0)
    , RecordId(0)
{ }

inline TVersion::TVersion(i32 segmentId, i32 recordId)
    : SegmentId(segmentId)
    , RecordId(recordId)
{ }

inline bool TVersion::operator < (TVersion other) const
{
    return
        SegmentId < other.SegmentId ||
        (SegmentId == other.SegmentId && RecordId < other.RecordId);
}

inline bool TVersion::operator == (TVersion other) const
{
    return SegmentId == other.SegmentId && RecordId == other.RecordId;
}

inline bool TVersion::operator != (TVersion other) const
{
    return !(*this == other);
}

inline bool TVersion::operator > (TVersion other) const
{
    return !(*this <= other);
}

inline bool TVersion::operator <= (TVersion other) const
{
    return *this < other || *this == other;
}

inline bool TVersion::operator >= (TVersion other) const
{
    return !(*this < other);
}

inline i64 TVersion::ToRevision() const
{
    return (static_cast<i64>(SegmentId) << 32) +
           static_cast<i64>(RecordId);
}

inline TVersion TVersion::FromRevision(i64 revision)
{
    return TVersion(revision >> 32, revision & 0xffffffff);
}

TVersion TVersion::Advance() const
{
    return TVersion(SegmentId, RecordId + 1);
}

TVersion TVersion::Rotate() const
{
    return TVersion(SegmentId + 1, 0);
}

inline Stroka ToString(TVersion version)
{
    return Sprintf("%d:%d", version.SegmentId, version.RecordId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

DECLARE_PODTYPE(NYT::NHydra::TVersion);
