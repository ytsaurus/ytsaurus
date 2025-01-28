#pragma once

#include <yt/yt/flow/lib/client/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

inline constexpr TStringBuf InputMessagesTableName = "input_messages";
inline constexpr TStringBuf OutputMessagesTableName = "output_messages";
inline constexpr TStringBuf CheckpointsTableName = "checkpoints";
inline constexpr TStringBuf TimerMessagesTableName = "timer_messages";
inline constexpr TStringBuf ControllerLogsTableName = "controller_logs";

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
