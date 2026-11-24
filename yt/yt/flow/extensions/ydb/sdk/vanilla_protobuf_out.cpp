// Adapted from https://github.com/ydb-platform/ydb-cpp-sdk/blob/v3.26.0/src/library/proto_output/proto_output.cpp
// Licensed under the Apache License, Version 2.0.

#include <contrib/ydb/public/api/protos/ydb_issue_message.pb.h>
#include <contrib/ydb/public/api/protos/ydb_topic.pb.h>
#include <contrib/ydb/public/api/protos/ydb_value.pb.h>

#include <util/stream/output.h>

#include <google/protobuf/text_format.h>

#include <string>

namespace {

void WriteShortUtf8DebugString(IOutputStream& stream, const google::protobuf::Message& message)
{
    google::protobuf::TextFormat::Printer printer;
    printer.SetSingleLineMode(true);
    printer.SetUseUtf8StringEscaping(true);

    std::string text;
    printer.PrintToString(message, &text);
    if (!text.empty() && text.back() == ' ') {
        text.pop_back();
    }

    stream << "{ " << text << " }";
}

} // namespace

Y_DECLARE_OUT_SPEC(, Ydb::Issue::IssueMessage, stream, value)
{
    WriteShortUtf8DebugString(stream, value);
}

Y_DECLARE_OUT_SPEC(, Ydb::VariantType, stream, value)
{
    WriteShortUtf8DebugString(stream, value);
}

Y_DECLARE_OUT_SPEC(, Ydb::Topic::StreamReadMessage_ReadResponse, stream, value)
{
    WriteShortUtf8DebugString(stream, value);
}

Y_DECLARE_OUT_SPEC(, Ydb::Topic::StreamReadMessage_CommitOffsetResponse, stream, value)
{
    WriteShortUtf8DebugString(stream, value);
}
