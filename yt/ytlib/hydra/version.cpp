#include "stdafx.h"
#include "version.h"

#include <core/misc/format.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

TVersion::TVersion()
    : SegmentId(0)
    , RecordId(0)
{ }

TVersion::TVersion(int segmentId, int recordId)
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
    return (static_cast<i64>(SegmentId) << 32) | static_cast<i64>(RecordId);
}

TVersion TVersion::FromRevision(i64 revision)
{
    return TVersion(revision >> 32, revision & 0xffffffff);
}

void TVersion::Advance(int delta)
{
    YASSERT(delta >= 0);
    RecordId += delta;
}

void TVersion::Rotate()
{
    ++SegmentId;
    RecordId = 0;
}

void FormatValue(TStringBuilder* builder, TVersion version)
{
    builder->AppendFormat("%v:%v", version.SegmentId, version.RecordId);
}

Stroka ToString(TVersion version)
{
    TStringBuilder builder;
    FormatValue(&builder, version);
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
