#pragma once

#include "public.h"

#include <yt/core/http/config.h>

namespace NYT {
namespace NRpc {
namespace NHttp {

////////////////////////////////////////////////////////////////////////////////

class TServerConfig
    : public NHttp::TServerConfig
{ };

DEFINE_REFCOUNTED_CLASS(TServerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttp
} // namespace NRpc
} // namespace NYT
