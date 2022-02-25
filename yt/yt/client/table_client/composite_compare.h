#pragma once

#include <yt/yt/core/misc/public.h>
#include <yt/yt/core/misc/farm_hash.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

int CompareCompositeValues(TStringBuf lhs, TStringBuf rhs);

TFingerprint CompositeHash(TStringBuf compositeValue);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
