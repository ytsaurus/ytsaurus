#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/computation/event_timestamp_assigner.h>

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TEventTimestampAssignerTest, Basic)
{
    auto schema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        [
            {name = "seconds"; type = "uint64"; required = %false;};
            {name = "milliseconds"; type = "uint64"; required = %false;};
            {name = "iso8601"; type = "string"; required = %false;};
        ]
    )""")));

    TMessageBuilder builder("stream", schema);
    builder.Payload().Set(41u, "seconds");
    builder.Payload().Set(42000u, "milliseconds");
    builder.Payload().Set("1970-01-01T00:00:45Z", "iso8601");
    const TMessage baseMessage = builder.Finish();

    {
        auto message = baseMessage;
        CreateEventTimestampAssigner(
            ConvertTo<TEventTimestampAssignerSpecPtr>(TYsonString(TStringBuf("{column=seconds; format=seconds}"))))
            ->Assign(message);
        EXPECT_EQ(message.EventTimestamp.Underlying(), 41u);
    }

    {
        auto message = baseMessage;
        CreateEventTimestampAssigner(
            ConvertTo<TEventTimestampAssignerSpecPtr>(TYsonString(TStringBuf("{column=milliseconds; format=milli_seconds}"))))
            ->Assign(message);
        EXPECT_EQ(message.EventTimestamp.Underlying(), 42u);
    }

    {
        auto message = baseMessage;
        CreateEventTimestampAssigner(
            ConvertTo<TEventTimestampAssignerSpecPtr>(TYsonString(TStringBuf("{column=iso8601; format=iso8601}"))))
            ->Assign(message);
        EXPECT_EQ(message.EventTimestamp.Underlying(), 45u);
    }

    {
        auto message = baseMessage;
        message.SystemTimestamp = TSystemTimestamp{40};
        CreateEventTimestampAssigner(
            ConvertTo<TEventTimestampAssignerSpecPtr>(TYsonString(TStringBuf("{column=seconds; format=seconds; limit_by_system_timestamp=%true}"))))
            ->Assign(message);
        EXPECT_EQ(message.EventTimestamp.Underlying(), 40u);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
