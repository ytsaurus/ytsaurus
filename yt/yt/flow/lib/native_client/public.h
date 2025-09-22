#pragma once

#include <yt/yt/flow/lib/client/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

inline constexpr TStringBuf InputMessagesTableName = "input_messages";
inline constexpr TStringBuf OutputMessagesTableName = "output_messages";
inline constexpr TStringBuf PartitionOutputMessagesTableName = "partition_output_messages";
inline constexpr TStringBuf StatesTableName = "states";
inline constexpr TStringBuf PartitionStatesTableName = "partition_states";
inline constexpr TStringBuf TimersTableName = "timers";
inline constexpr TStringBuf ControllerLogsTableName = "controller_logs";
inline constexpr TStringBuf FlowStateTableName = "flow_state";
inline constexpr TStringBuf FlowStateObsoleteTableName = "flow_state_obsolete";
inline constexpr TStringBuf PartitionTransactionsTableName = "partition_transactions";

inline const std::vector InternalFlowTables{
    InputMessagesTableName,
    OutputMessagesTableName,
    PartitionOutputMessagesTableName,
    StatesTableName,
    PartitionStatesTableName,
    TimersTableName,
    ControllerLogsTableName,
    FlowStateTableName,
    FlowStateObsoleteTableName,
    PartitionTransactionsTableName};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
