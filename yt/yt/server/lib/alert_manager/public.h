#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NAlertManager {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IAlertManager)
DECLARE_REFCOUNTED_STRUCT(TAlertManagerDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(IAlertCollector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAlertManager
