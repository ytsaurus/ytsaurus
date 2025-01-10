#pragma once

#include "public.h"

#include <yt/yt/library/query/base/public.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

NTableClient::IGranuleFilterPtr CreateGranuleMinMaxFilter(
    const NQueryClient::TConstQueryPtr& query,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
