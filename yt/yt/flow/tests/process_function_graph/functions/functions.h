#pragma once

#include <yt/yt/flow/library/cpp/common/yson_message.h>

namespace NYT::NFlow::NProcessFunctionGraphTest {

////////////////////////////////////////////////////////////////////////////////

//! A message of the input streams `left` and `right`.
struct TKeyMessage
    : public TYsonMessage
{
    ui64 Key = 0;

    REGISTER_YSON_STRUCT(TKeyMessage);

    static void Register(TRegistrar registrar);
};

//! A message of the internal streams between the counters and the merge node.
struct TStageMessage
    : public TYsonMessage
{
    ui64 Key = 0;
    i64 BranchCount = 0;
    i64 Kind = 0;

    REGISTER_YSON_STRUCT(TStageMessage);

    static void Register(TRegistrar registrar);
};

//! A message of the output stream `result`.
struct TFinalMessage
    : public TYsonMessage
{
    ui64 Key = 0;
    i64 Count = 0;
    i64 BranchCount = 0;
    i64 Kind = 0;

    REGISTER_YSON_STRUCT(TFinalMessage);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NProcessFunctionGraphTest
