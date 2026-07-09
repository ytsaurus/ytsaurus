#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TBasicMessage
    : public TYsonMessage
{
    ui64 BannerId{};
    bool IsClick{};
    std::optional<std::string> Title;
    std::optional<std::string> Empty;

    REGISTER_YSON_STRUCT(TBasicMessage);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("banner_id", &TThis::BannerId)
            .Default();
        registrar.Parameter("is_click", &TThis::IsClick)
            .Default(false);
        registrar.Parameter("title", &TThis::Title)
            .Default();
        registrar.Parameter("empty", &TThis::Empty)
            .Default();
    }
};

using TBasicMessagePtr = TIntrusivePtr<TBasicMessage>;

YT_FLOW_DEFINE_YSON_MESSAGE(TBasicMessage);

TEST(TMessageSerializeTest, Basic)
{
    auto expectedSchema = New<TTableSchema>(
        std::vector{
            TColumnSchema("banner_id", EValueType::Uint64).SetRequired(false),
            TColumnSchema("empty", EValueType::String).SetRequired(false),
            TColumnSchema("is_click", EValueType::Boolean).SetRequired(false),
            TColumnSchema("title", EValueType::String).SetRequired(false),
        });

    auto schema = GetYsonMessagePayloadSchema<TBasicMessage>();
    EXPECT_EQ(*expectedSchema, *schema);
    auto ysonMessage = New<TBasicMessage>();
    ysonMessage->Meta->MessageId = TMessageId("some_message_id");
    ysonMessage->Meta->SystemTimestamp = TSystemTimestamp(100500ull);
    ysonMessage->Meta->AlignmentTimestamp = TSystemTimestamp(100400ull);
    ysonMessage->Meta->EventTimestamp = TSystemTimestamp(100300ull);
    ysonMessage->Meta->StreamId = TStreamId("some_stream");
    ysonMessage->BannerId = 523;
    ysonMessage->IsClick = true;
    ysonMessage->Title = "some_title";

    auto message = ConvertToMessage(ysonMessage, schema);
    EXPECT_EQ(message.MessageId.Underlying(), "some_message_id");
    EXPECT_EQ(message.SystemTimestamp.Underlying(), 100500ull);
    EXPECT_EQ(message.AlignmentTimestamp.Underlying(), 100400ull);
    EXPECT_EQ(message.EventTimestamp.Underlying(), 100300ull);
    EXPECT_EQ(message.StreamId, "some_stream");
    EXPECT_EQ(GetColumnValue<ui64>(message, "banner_id"), 523ull);
    EXPECT_EQ(GetColumnValue<bool>(message, "is_click"), true);
    EXPECT_EQ(GetColumnValue<std::optional<std::string>>(message, "title"), "some_title");
    EXPECT_EQ(GetColumnValue<std::optional<std::string>>(message, "empty"), std::nullopt);

    auto ysonMessageCopy = TRegistry::Get()->CreateYsonMessage(TypeName<TBasicMessage>());
    TRegistry::Get()->ValidateYsonMessageType(TypeName<TBasicMessage>(), ysonMessageCopy);
    EXPECT_TRUE(DynamicPointerCast<TBasicMessage>(ysonMessageCopy));
    ConvertToYsonMessage(message, ysonMessageCopy);
    EXPECT_TRUE(ysonMessageCopy->IsEqual(*ysonMessage));

    auto node = NYTree::ConvertToNode(ysonMessageCopy);
    // clang-format off
    auto expectedNode = NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("$meta")
                .BeginMap()
                    .Item("message_id").Value("some_message_id")
                    .Item("system_timestamp").Value(100500ull)
                    .Item("alignment_timestamp").Value(100400ull)
                    .Item("event_timestamp").Value(100300ull)
                    .Item("stream_id").Value("some_stream")
                .EndMap()
            .Item("banner_id").Value(523ull)
            .Item("is_click").Value(true)
            .Item("title").Value("some_title")
        .EndMap();
    // clang-format on
    EXPECT_TRUE(NYTree::AreNodesEqual(node, expectedNode));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
