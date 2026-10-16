#pragma once

#include "resource_base.h"

#include <yt/yt/client/api/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Resource that exposes an ``NApi::IClientPtr`` for use as a custom YT client
//! by external state managers/readers via ``client_provider_resource_id`` in
//! their spec.
struct IYTClientProvider
    : public TResourceBase
{
    using TResourceBase::TResourceBase;

    virtual NApi::IClientPtr Get() = 0;
};

DEFINE_REFCOUNTED_TYPE(IYTClientProvider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
