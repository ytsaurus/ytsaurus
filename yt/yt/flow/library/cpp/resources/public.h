#pragma once

#include <yt/yt/flow/library/cpp/common/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

inline const TResourceId YTClientFactoryDefaultResourceId = "YTClientFactory";
inline const TResourceId YTHedgingClientDefaultResourceId = "YTHedgingClient";

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IYTClientFactory);
DECLARE_REFCOUNTED_CLASS(TYTClientFactory);
DECLARE_REFCOUNTED_CLASS(TYTHedgingClient);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
