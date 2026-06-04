#pragma once

#include <yt/yt/flow/lib/client/public.h>

#include <yt/yt/client/ypath/rich.h>

#include <library/cpp/yt/string/format.h>

#include <string>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Cypress attribute on a pipeline node that points to the vanilla operation
//! currently running it (a map with "alias" and optional "runtime_cluster").
//! Lets the runner find and shut down a previous launch before starting a new one.
inline constexpr TStringBuf CurrentVanillaOperationAttribute = "current_vanilla_operation";

//! Default alias for a Flow vanilla operation: pins the op to the (cluster, pipeline)
//! pair so a second launch of the same pipeline is rejected by the scheduler instead
//! of racing with a stray previous run.
inline std::string BuildVanillaOperationAlias(const NYPath::TRichYPath& pipelinePath)
{
    // The leading "*" is a YT scheduler convention — every operation alias must start with it.
    // The "<cluster>:<path>" body is the canonical short form of a rich YPath.
    return Format("*flow-runner %v:%v", pipelinePath.GetCluster().value_or(""), pipelinePath.GetPath());
}

////////////////////////////////////////////////////////////////////////////////

inline constexpr TStringBuf InputMessagesTableName = "input_messages";
inline constexpr TStringBuf CompactInputMessagesTableName = "compact_input_messages";
inline constexpr TStringBuf OutputMessagesTableName = "output_messages";
inline constexpr TStringBuf PartitionOutputMessagesTableName = "partition_output_messages";
inline constexpr TStringBuf CompactPartitionOutputMessagesTableName = "compact_partition_output_messages";
inline constexpr TStringBuf CompactOutputMessagesTableName = "compact_output_messages";
inline constexpr TStringBuf StatesTableName = "states";
inline constexpr TStringBuf PartitionStatesTableName = "partition_states";
inline constexpr TStringBuf KeyVisitorStatesTableName = "key_visitor_states";
inline constexpr TStringBuf TimersTableName = "timers";
inline constexpr TStringBuf ControllerLogsTableName = "controller_logs";
inline constexpr TStringBuf FlowStateTableName = "flow_state";
inline constexpr TStringBuf FlowStateObsoleteTableName = "flow_state_obsolete";
inline constexpr TStringBuf PartitionTransactionsTableName = "partition_transactions";

inline const std::vector InternalFlowTables{
    InputMessagesTableName,
    CompactInputMessagesTableName,
    OutputMessagesTableName,
    PartitionOutputMessagesTableName,
    CompactPartitionOutputMessagesTableName,
    CompactOutputMessagesTableName,
    StatesTableName,
    PartitionStatesTableName,
    KeyVisitorStatesTableName,
    TimersTableName,
    ControllerLogsTableName,
    FlowStateTableName,
    FlowStateObsoleteTableName,
    PartitionTransactionsTableName};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
