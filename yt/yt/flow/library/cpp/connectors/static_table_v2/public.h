#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

#include <library/cpp/yt/misc/strong_typedef.h>

namespace NYT::NFlow::NStaticTableConnectorV2 {

////////////////////////////////////////////////////////////////////////////////

class TSourceController;

DECLARE_REFCOUNTED_STRUCT(TTableTimestampLocatorSpec);
DECLARE_REFCOUNTED_STRUCT(TTableSourceParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicTableSourceParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicTableSourcePartitionSpec);

DECLARE_REFCOUNTED_STRUCT(TPartitionStatus);

DECLARE_REFCOUNTED_STRUCT(TSourceControllerTable);
DECLARE_REFCOUNTED_STRUCT(TClusterProgress);
DECLARE_REFCOUNTED_STRUCT(TEventNameOrder);
DECLARE_REFCOUNTED_STRUCT(TSourceControllerState);

YT_DEFINE_STRONG_TYPEDEF(TRangeId, i64);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NStaticTableConnectorV2
