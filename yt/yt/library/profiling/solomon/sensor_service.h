#pragma once

#include "public.h"

#include <yt/yt/core/ytree/ypath_detail.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

NYTree::IYPathServicePtr CreateSensorService(TSolomonExporterConfigPtr config, TSolomonRegistryPtr registry, TSolomonExporterPtr exporter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
