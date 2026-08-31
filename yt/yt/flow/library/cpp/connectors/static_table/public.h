#pragma once

#include <yt/yt/flow/library/cpp/connectors/static_table_v2/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NFlow::NStaticTableConnector {

////////////////////////////////////////////////////////////////////////////////

class TArrivalOrderTableSink;
class TArrivalOrderTableSinkController;

DECLARE_REFCOUNTED_STRUCT(TArrivalOrderTableSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicArrivalOrderTableSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TArrivalOrderTableSinkOwner);
DECLARE_REFCOUNTED_STRUCT(TArrivalOrderTableSinkPartitionProgress);
DECLARE_REFCOUNTED_STRUCT(TArrivalOrderTableSinkProgress);

using TSourceController = NStaticTableConnectorV2::TSourceController;
using TTableTimestampLocatorSpec = NStaticTableConnectorV2::TTableTimestampLocatorSpec;
using TTableTimestampLocatorSpecPtr = NStaticTableConnectorV2::TTableTimestampLocatorSpecPtr;
using TTableSourceParameters = NStaticTableConnectorV2::TTableSourceParameters;
using TTableSourceParametersPtr = NStaticTableConnectorV2::TTableSourceParametersPtr;
using TDynamicTableSourceParameters = NStaticTableConnectorV2::TDynamicTableSourceParameters;
using TDynamicTableSourceParametersPtr = NStaticTableConnectorV2::TDynamicTableSourceParametersPtr;
using TDynamicTableSourcePartitionSpec = NStaticTableConnectorV2::TDynamicTableSourcePartitionSpec;
using TDynamicTableSourcePartitionSpecPtr = NStaticTableConnectorV2::TDynamicTableSourcePartitionSpecPtr;
using TPartitionStatus = NStaticTableConnectorV2::TPartitionStatus;
using TPartitionStatusPtr = NStaticTableConnectorV2::TPartitionStatusPtr;
using TSourceControllerTable = NStaticTableConnectorV2::TSourceControllerTable;
using TSourceControllerTablePtr = NStaticTableConnectorV2::TSourceControllerTablePtr;
using TSourceControllerState = NStaticTableConnectorV2::TSourceControllerState;
using TSourceControllerStatePtr = NStaticTableConnectorV2::TSourceControllerStatePtr;
using TRangeId = NStaticTableConnectorV2::TRangeId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NStaticTableConnector
