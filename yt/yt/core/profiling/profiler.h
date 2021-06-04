#pragma once

#include "tscp.h"

#include <yt/yt/core/misc/optional.h>
#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/ref_tracked.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/concurrency/spinlock.h>

#include <atomic>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

TTagIdList  operator +  (const TTagIdList& a, const TTagIdList& b);
TTagIdList& operator += (TTagIdList& a, const TTagIdList& b);

////////////////////////////////////////////////////////////////////////////////

TCpuInstant GetCpuInstant();
TValue CpuDurationToValue(TCpuDuration duration);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling

////////////////////////////////////////////////////////////////////////////////

//! A hasher for TTagIdList.
template <>
struct THash<NYT::NProfiling::TTagIdList>
{
    size_t operator()(const NYT::NProfiling::TTagIdList& ids) const;
};

////////////////////////////////////////////////////////////////////////////////
