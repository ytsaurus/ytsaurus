#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

#include <library/cpp/yt/misc/strong_typedef.h>

namespace NYT::NFlow::NStaticTableConnector {

////////////////////////////////////////////////////////////////////////////////

class TSourceController;
class TArrivalOrderTableSink;
class TArrivalOrderTableSinkController;

DECLARE_REFCOUNTED_STRUCT(TTableTimestampLocatorSpec);
DECLARE_REFCOUNTED_STRUCT(TTableSourceParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicTableSourceParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicTableSourcePartitionSpec);
DECLARE_REFCOUNTED_STRUCT(TArrivalOrderTableSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicArrivalOrderTableSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TArrivalOrderTableSinkOwner);
DECLARE_REFCOUNTED_STRUCT(TArrivalOrderTableSinkPartitionProgress);
DECLARE_REFCOUNTED_STRUCT(TArrivalOrderTableSinkProgress);

DECLARE_REFCOUNTED_STRUCT(TPartitionStatus);

DECLARE_REFCOUNTED_STRUCT(TSourceControllerTable);
DECLARE_REFCOUNTED_STRUCT(TSourceControllerState);

YT_DEFINE_STRONG_TYPEDEF(TRangeId, i64);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NStaticTableConnector
