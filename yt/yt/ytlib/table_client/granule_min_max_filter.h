#pragma once

#include "public.h"

#include <yt/yt/library/query/base/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

NTableClient::IGranuleFilterPtr CreateGranuleMinMaxFilter(
    const NQueryClient::TConstQueryPtr& query);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
