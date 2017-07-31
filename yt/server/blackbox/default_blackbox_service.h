#pragma once

#include "blackbox_service.h"
#include "config.h"

namespace NYT {
namespace NBlackbox {

////////////////////////////////////////////////////////////////////////////////

IBlackboxServicePtr CreateDefaultBlackboxService(
    TDefaultBlackboxServiceConfigPtr config,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NBlackbox
} // namespace NYT
