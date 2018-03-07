#include "version.h"

#include <yt/core/misc/format.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

TVersion::TVersion() noexcept
    : SegmentId(0)
    , RecordId(0)
{ }

TVersion::TVersion(int segmentId, int recordId) noexcept
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

TVersion TVersion::Advance(int delta) const
{
    Y_ASSERT(delta >= 0);
    return TVersion(SegmentId, RecordId + delta);
}

TVersion TVersion::Rotate() const
{
    return TVersion(SegmentId + 1, 0);
}

void FormatValue(TStringBuilder* builder, TVersion version, const TStringBuf& /*spec*/)
{
    builder->AppendFormat("%v:%v", version.SegmentId, version.RecordId);
}

TString ToString(TVersion version)
{
    return ToStringViaBuilder(version);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
