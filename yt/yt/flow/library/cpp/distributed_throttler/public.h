#pragma once

#include <yt/yt/flow/library/cpp/common/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <util/generic/fwd.h>

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

inline const TQuotaClassId DefaultQuotaClassId = NFlow::DefaultQuotaClassName;

DECLARE_REFCOUNTED_STRUCT(IDistributedThrottlerService);
DECLARE_REFCOUNTED_STRUCT(TDistributedThrottlerServiceConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
