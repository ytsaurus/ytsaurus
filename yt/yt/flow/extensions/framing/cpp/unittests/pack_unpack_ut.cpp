#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/flow/extensions/framing/cpp/packer.h>
#include <yt/yt/flow/extensions/framing/cpp/unpacker.h>

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/extensions/framing/cpp/unittests/proto/message_example_ut.pb.h>
#include <yt/yt/flow/extensions/framing/cpp/unittests/proto_filler.h>

#include <library/cpp/blockcodecs/codecs.h>
#include <library/cpp/yt/yson_string/string.h>

namespace NYT::NFlow {
namespace {

using ::testing::_;
using ::testing::Return;

////////////////////////////////////////////////////////////////////////////////

static const auto Schema = ConvertTo<NTableClient::TTableSchemaPtr>(NYson::TYsonString(TStringBuf(R"""(
    [
        {name = "data"; type = "string"; required = %true;};
    ]
)""")));

////////////////////////////////////////////////////////////////////////////////

class TProtoPackerUnpackerTest
    : public ::testing::Test
{
public:
    void SetUp() override
    {
        Reset();
    }

    void Reset()
    {
        FramingFormat = EFramingFormat::Protoseq;
        MaxRowsPerMessage = 1000;
        MaxBytesPerMessage = 1024 * 1024;

        ProtoFiller = NProtoPackerTest::TProtoFiller();
    }

    std::vector<NProto::TExampleMessage> CreateTestMessages(i32 count)
    {
        std::vector<NProto::TExampleMessage> messages;
        for (i32 i = 0; i < count; ++i) {
            NProto::TExampleMessage proto;
            ProtoFiller.FillMessage(proto);
            messages.push_back(proto);
        }
        return messages;
    }

    std::vector<NProto::TExampleMessage> ExtractProtoMessages(const std::vector<TMessage>& messages)
    {
        std::vector<NProto::TExampleMessage> result;

        for (const auto& message : messages) {
            auto data = GetColumnValue<TStringBuf>(message, "data");
            NProto::TExampleMessage proto;
            EXPECT_TRUE(proto.ParseFromArray(data.data(), data.size()));
            result.push_back(proto);
        }

        return result;
    }

    std::vector<TMessage> PackMessages(const std::vector<NProto::TExampleMessage>& sourceMessages)
    {
        TMessageBuilder builder("stream_1", Schema);
        std::deque<TOutputMessageConstPtr> convertedMessages;

        auto streamSpec = New<TStreamSpec>();
        streamSpec->Schema = Schema;
        THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> specs;
        specs[TStreamId("stream_1")][TStreamSpecId(1)] = streamSpec;
        auto specStorage = New<TComputationStreamSpecStorage>(
            New<TStreamSpecs>(specs),
            /*groupBySchema*/ New<NTableClient::TTableSchema>(),
            /*evaluatorCache*/ nullptr);

        for (size_t i = 0; i < sourceMessages.size(); ++i) {
            builder.Payload().Set(SerializeProtoToString(sourceMessages[i], false), "data");
            builder.SetMessageId(TMessageId(ToString(i)));
            builder.SetSystemTimestamp(TSystemTimestamp(1700000000));
            builder.SetAlignmentTimestamp(TSystemTimestamp(1700000000));
            builder.SetEventTimestamp(TSystemTimestamp(1700000000));
            convertedMessages.push_back(New<TOutputMessage>(builder.Finish(), specStorage));
        }

        auto batchedMessages = PackBatches(
            convertedMessages,
            "data",
            FramingFormat,
            MaxRowsPerMessage,
            MaxBytesPerMessage);

        TMessageBuilder packBuilder("stream_1", Schema);
        std::vector<TMessage> allOutputMessages;
        for (auto& [data, _] : batchedMessages) {
            packBuilder.Payload().Set(data, "data");
            allOutputMessages.push_back(packBuilder.Finish());
        }

        return allOutputMessages;
    }

    std::vector<NProto::TExampleMessage> UnpackMessages(const std::vector<TMessage>& packedMessages)
    {
        std::vector<TMessage> allUnpackedMessages;
        for (const auto& packedMessage : packedMessages) {
            std::string rawData = GetColumnValue<std::string>(packedMessage, "data");
            auto result = UnpackBatch<NProto::TExampleMessage>(
                rawData,
                FramingFormat);

            TMessageBuilder unpackBuilder("stream_1", Schema);
            if (result.IsOK()) {
                for (auto& proto : result.Value()) {
                    unpackBuilder.Payload().Set(SerializeProtoToString(proto, false), "data");
                    allUnpackedMessages.push_back(unpackBuilder.Finish());
                }
            } else {
                THROW_ERROR_EXCEPTION("Failed to unpack message");
            }
        }

        return ExtractProtoMessages(allUnpackedMessages);
    }

    void VerifyMessages(const std::vector<NProto::TExampleMessage>& sourceMessages,
        const std::vector<NProto::TExampleMessage>& unpackedMessages)
    {
        ASSERT_EQ(unpackedMessages.size(), sourceMessages.size());

        for (size_t i = 0; i < sourceMessages.size(); ++i) {
            EXPECT_EQ(SerializeProtoToString(sourceMessages[i], false), SerializeProtoToString(unpackedMessages[i], false));
        }
    }

public:
    EFramingFormat FramingFormat{};
    i64 MaxBytesPerMessage{};
    i64 MaxRowsPerMessage{};
    NProtoPackerTest::TProtoFiller ProtoFiller;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TProtoPackerUnpackerTest, PackAndUnpack)
{
    const i32 messageCount = 10;
    auto sourceMessages = CreateTestMessages(messageCount);

    auto packedMessages = PackMessages(sourceMessages);
    ASSERT_GT(packedMessages.size(), 0U);

    auto unpackedMessages = UnpackMessages(packedMessages);
    VerifyMessages(sourceMessages, unpackedMessages);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TProtoPackerUnpackerTest, VerifyPackDynamicSpecLimits)
{
    const i32 messageCount = 100;
    auto sourceMessages = CreateTestMessages(messageCount);

    // Verify MaxRowsPerMessage limit
    {
        Reset();
        MaxRowsPerMessage = 10;
        MaxBytesPerMessage = 1024 * 1024;

        auto packedMessages = PackMessages(sourceMessages);

        EXPECT_GE(std::ssize(packedMessages), messageCount / MaxRowsPerMessage);

        auto unpackedMessages = UnpackMessages(packedMessages);
        VerifyMessages(sourceMessages, unpackedMessages);
    }

    // Verify MaxBytesPerMessage limit
    {
        Reset();
        MaxRowsPerMessage = 1000;
        MaxBytesPerMessage = 5000;

        auto packedMessages = PackMessages(sourceMessages);

        EXPECT_GT(std::ssize(packedMessages), 1U);

        i64 maxMessageSize = 0;
        for (const auto& message : packedMessages) {
            auto data = GetColumnValue<TStringBuf>(message, "data");
            maxMessageSize = std::max(maxMessageSize, std::ssize(data));
        }

        // Verify that each packed message doesn't exceed the limit by more than
        // a single message size plus the MaxBytesPerMessage.
        i64 upperBound = MaxBytesPerMessage + maxMessageSize;
        for (const auto& message : packedMessages) {
            auto data = GetColumnValue<TStringBuf>(message, "data");
            EXPECT_LE(std::ssize(data), upperBound);
        }

        auto unpackedMessages = UnpackMessages(packedMessages);
        VerifyMessages(sourceMessages, unpackedMessages);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TProtoPackerUnpackerTest, VerifyBorderValues)
{
    const int messageCount = 20;
    auto sourceMessages = CreateTestMessages(messageCount);

    {
        Reset();
        MaxRowsPerMessage = 1;
        MaxBytesPerMessage = 1024 * 1024;

        auto packedMessages = PackMessages(sourceMessages);
        EXPECT_GE(std::ssize(packedMessages), messageCount);

        auto unpackedMessages = UnpackMessages(packedMessages);
        VerifyMessages(sourceMessages, unpackedMessages);
    }

    {
        Reset();
        MaxRowsPerMessage = 0;
        MaxBytesPerMessage = 0;

        auto packedMessages = PackMessages(sourceMessages);
        auto unpackedMessages = UnpackMessages(packedMessages);
        VerifyMessages(sourceMessages, unpackedMessages);
    }

    {
        Reset();
        MaxBytesPerMessage = 10;
        MaxRowsPerMessage = 1000;

        auto packedMessages = PackMessages(sourceMessages);
        auto unpackedMessages = UnpackMessages(packedMessages);
        VerifyMessages(sourceMessages, unpackedMessages);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
