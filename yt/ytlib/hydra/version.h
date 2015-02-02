#pragma once

#include "public.h"

#include <core/misc/string.h>

#include <util/generic/typetraits.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TVersion
{
    int SegmentId;
    int RecordId;

    TVersion() noexcept;
    TVersion(int segmentId, int recordId) noexcept;

    bool operator < (TVersion other) const;
    bool operator == (TVersion other) const;
    bool operator != (TVersion other) const;
    bool operator > (TVersion other) const;
    bool operator <= (TVersion other) const;
    bool operator >= (TVersion other) const;

    i64 ToRevision() const;
    static TVersion FromRevision(i64 revision);

    TVersion Advance(int delta = 1);
    TVersion Rotate();

};

void FormatValue(TStringBuilder* builder, TVersion version);
Stroka ToString(TVersion version);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

DECLARE_PODTYPE(NYT::NHydra::TVersion);
