#pragma once

#include <yt/yt/flow/library/cpp/resources/resource_base.h>
#include <yt/yt/flow/library/cpp/resources/yt_client_provider.h>

#include <yt/yt/client/hedging/hedging.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct IYTHedgingClient
    : public IYTClientProvider
{
    using IYTClientProvider::IYTClientProvider;
};

DEFINE_REFCOUNTED_TYPE(IYTHedgingClient);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
