#pragma once

#include <yt/yt/flow/library/cpp/connectors/static_table/public.h>

namespace NYT::NFlow::NStaticTableConnectorV2 {

////////////////////////////////////////////////////////////////////////////////

using EMigrationMode = NStaticTableConnector::EMigrationMode;
using TSourceController = NStaticTableConnector::TSourceController;
using TTableTimestampLocatorSpec = NStaticTableConnector::TTableTimestampLocatorSpec;
using TTableTimestampLocatorSpecPtr = NStaticTableConnector::TTableTimestampLocatorSpecPtr;
using TTableSourceParameters = NStaticTableConnector::TTableSourceParameters;
using TTableSourceParametersPtr = NStaticTableConnector::TTableSourceParametersPtr;
using TDynamicTableSourceParameters = NStaticTableConnector::TDynamicTableSourceParameters;
using TDynamicTableSourceParametersPtr = NStaticTableConnector::TDynamicTableSourceParametersPtr;
using TDynamicTableSourcePartitionSpec = NStaticTableConnector::TDynamicTableSourcePartitionSpec;
using TDynamicTableSourcePartitionSpecPtr = NStaticTableConnector::TDynamicTableSourcePartitionSpecPtr;
using TPartitionStatus = NStaticTableConnector::TPartitionStatus;
using TPartitionStatusPtr = NStaticTableConnector::TPartitionStatusPtr;
using TSourceControllerTable = NStaticTableConnector::TSourceControllerTable;
using TSourceControllerTablePtr = NStaticTableConnector::TSourceControllerTablePtr;
using TClusterProgress = NStaticTableConnector::TClusterProgress;
using TClusterProgressPtr = NStaticTableConnector::TClusterProgressPtr;
using TEventNameOrder = NStaticTableConnector::TEventNameOrder;
using TEventNameOrderPtr = NStaticTableConnector::TEventNameOrderPtr;
using TSourceControllerState = NStaticTableConnector::TSourceControllerState;
using TSourceControllerStatePtr = NStaticTableConnector::TSourceControllerStatePtr;
using TRangeId = NStaticTableConnector::TRangeId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NStaticTableConnectorV2
