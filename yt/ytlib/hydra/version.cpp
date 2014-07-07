#include "stdafx.h"
#include "version.h"

#include <core/misc/format.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

const TVersion InvalidVersion = TVersion(0, -1);

TVersion::TVersion()
    : SegmentId(0)
    , RecordId(0)
{ }

TVersion::TVersion(i32 segmentId, i32 recordId)
    : SegmentId(segmentId)
    , RecordId(recordId)
{ }

bool TVersion::operator < (TVersion other) const
{
    return
        SegmentId < other.SegmentId ||
        (SegmentId == other.SegmentId && RecordId < other.RecordId);
}

bool TVersion::operator == (TVersion other) const
{
    return SegmentId == other.SegmentId && RecordId == other.RecordId;
}

bool TVersion::operator != (TVersion other) const
{
    return !(*this == other);
}

bool TVersion::operator > (TVersion other) const
{
    return !(*this <= other);
}

bool TVersion::operator <= (TVersion other) const
{
    return *this < other || *this == other;
}

bool TVersion::operator >= (TVersion other) const
{
    return !(*this < other);
}

i64 TVersion::ToRevision() const
{
    return IsValid()
        ? (static_cast<i64>(SegmentId) << 32) | static_cast<i64>(RecordId)
        : -1;
}

TVersion TVersion::FromRevision(i64 revision)
{
    return TVersion(revision >> 32, revision & 0xffffffff);
}

bool TVersion::IsValid() const
{
    return SegmentId >= 0 && RecordId >= 0;
}

Stroka ToString(TVersion version)
{
    return Format("%v:%v", version.SegmentId, version.RecordId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
