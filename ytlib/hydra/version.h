#pragma once

#include "public.h"

#include <yt/core/misc/string.h>

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

    TVersion Advance(int delta = 1) const;
    TVersion Rotate() const;
};

void FormatValue(TStringBuilder* builder, TVersion version, const TStringBuf& spec);
TString ToString(TVersion version);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

Y_DECLARE_PODTYPE(NYT::NHydra::TVersion);
