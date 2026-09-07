#pragma once

#include <yt/yt/flow/examples/cpp/proto_parser/proto/log_record.pb.h>

#include <yt/yt/flow/library/cpp/common/state_client.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/flow/library/cpp/parsers/proto.h>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

struct TLogRecordMessage
    : public TYsonMessage
{
    std::string Level;
    std::string Text;
    i64 SeenAtLevel = 0;

    REGISTER_YSON_STRUCT(TLogRecordMessage);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TLevelCountsState
    : public NYTree::TYsonStruct
{
    THashMap<std::string, i64> RecordCounts;

    REGISTER_YSON_STRUCT(TLevelCountsState);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TProtoLogParserFunction
    : public TProtoParsingProcessFunctionBase<
          TLogRecordProto,
          TProtoParsingProcessFunctionParameters,
          /*PropagateHookErrors*/ true>
{
protected:
    void DoInit(const IRuntimeInitContextPtr& initContext) override;

    void ProcessProto(
        const TInputMessageConstPtr& message,
        TLogRecordProto&& proto,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;

    void ProcessUnparsed(
        const TInputMessageConstPtr& message,
        TError error,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;

private:
    TMutableStateKeyClient<TLevelCountsState> StateClient_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NExample
