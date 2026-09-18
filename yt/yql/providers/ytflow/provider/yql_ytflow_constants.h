#pragma once

#include <util/generic/strbuf.h>


namespace NYql {

inline constexpr TStringBuf YTFLOW_COMBINED_STATE_FIELD = "_ytflow_combined_state";
inline constexpr TStringBuf YTFLOW_INPUT_MESSAGE_ID_FIELD = "$input_message_id";

inline constexpr TStringBuf YTFLOW_SUBDIRECTORY = "yql_ytflow";

inline constexpr TStringBuf YT_CONSUMERS_SUBDIRECTORY = "consumers";
inline constexpr TStringBuf YT_PRODUCERS_SUBDIRECTORY = "producers";

inline constexpr TStringBuf DEFAULT_YT_CONSUMER_NAME = "default_consumer";
inline constexpr TStringBuf DEFAULT_YT_PRODUCER_NAME = "default_producer";

inline constexpr TStringBuf INJECT_INPUT_MESSAGE_ID_SETTING = "inject_input_message_id";
inline constexpr TStringBuf EXTEND_SETTING = "extend";

} // namespace NYql
