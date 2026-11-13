#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TProcessFunctionStaticTableExportParameters);
DECLARE_REFCOUNTED_STRUCT(IStaticTableExportPartitionSource);
DECLARE_REFCOUNTED_STRUCT(IStaticTableExportControllerSource);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
