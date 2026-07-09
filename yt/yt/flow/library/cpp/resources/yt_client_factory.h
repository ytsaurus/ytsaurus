#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/resources/resource_base.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/cache/cache.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct IYTClientFactory
    : public TResourceBase
    , public NClient::NCache::IClientsCache
{
    using TResourceBase::TResourceBase;
};

DEFINE_REFCOUNTED_TYPE(IYTClientFactory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
