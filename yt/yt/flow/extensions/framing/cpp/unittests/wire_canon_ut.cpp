#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/flow/extensions/framing/cpp/packer.h>
#include <yt/yt/flow/extensions/framing/cpp/unpacker.h>

#include <yt/yt/flow/extensions/framing/cpp/unittests/proto/message_example_ut.pb.h>

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <library/cpp/framing/packer.h>
#include <library/cpp/framing/syncword.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/resource/resource.h>

#include <util/stream/str.h>
#include <util/string/hex.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf SourceSyncWordHex = "1ff7f77ebea65e9e37a6f62efeae47a7b76ebfaf169e9f37f657f766a706aff7";

const std::vector<EFramingFormat> Formats = {
    EFramingFormat::Auto,
    EFramingFormat::Protoseq,
    EFramingFormat::Lenval,
    EFramingFormat::LightProtoseq,
};

TString ToHex(TStringBuf data)
{
    return to_lower(HexEncode(data));
}

NJson::TJsonValue ReadCorpus()
{
    auto corpus = NResource::Find("wire-canon.json");
    TStringInput input(corpus);
    return NJson::ReadJsonTree(&input, /*throwOnError*/ true);
}

std::vector<TOutputMessageConstPtr> MakeOutputMessages(const std::vector<TString>& payloads)
{
    auto schema = New<NTableClient::TTableSchema>(std::vector{
        NTableClient::TColumnSchema("data", NTableClient::EValueType::String),
    });
    auto streamSpec = New<TStreamSpec>();
    streamSpec->Schema = schema;
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> specs;
    specs[TStreamId("stream")][TStreamSpecId(1)] = streamSpec;
    auto specStorage = New<TComputationStreamSpecStorage>(
        New<TStreamSpecs>(specs),
        /*groupBySchema*/ New<NTableClient::TTableSchema>(),
        /*evaluatorCache*/ nullptr);

    std::vector<TOutputMessageConstPtr> messages;
    TMessageBuilder builder("stream", schema);
    for (int index = 0; index < std::ssize(payloads); ++index) {
        builder.Payload().Set(TStringBuf(payloads[index]), "data");
        builder.SetMessageId(TMessageId(ToString(index)));
        builder.SetSystemTimestamp(TSystemTimestamp(1700000000));
        builder.SetAlignmentTimestamp(TSystemTimestamp(1700000000));
        builder.SetEventTimestamp(TSystemTimestamp(1700000000));
        messages.push_back(New<TOutputMessage>(builder.Finish(), specStorage));
    }
    return messages;
}

std::vector<TString> MakeProtoPayloads(int count)
{
    std::vector<TString> payloads;
    for (int index = 0; index < count; ++index) {
        NProto::TExampleMessage message;
        message.set_int32_field(index);
        message.set_string_field(TString(index, '\xff'));
        payloads.push_back(SerializeProtoToString(message, false));
    }
    return payloads;
}

std::vector<TString> SerializeAll(const std::vector<NProto::TExampleMessage>& messages)
{
    std::vector<TString> result;
    for (const auto& message : messages) {
        result.push_back(SerializeProtoToString(message, false));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TFramingWireCanonTest, EnumValuesAndMinimalFrames)
{
    EXPECT_EQ(static_cast<int>(EFramingFormat::Auto), 0);
    EXPECT_EQ(static_cast<int>(EFramingFormat::Protoseq), 1);
    EXPECT_EQ(static_cast<int>(EFramingFormat::Lenval), 2);
    EXPECT_EQ(static_cast<int>(EFramingFormat::LightProtoseq), 3);
    EXPECT_EQ(NFraming::PackToString(NFraming::EFormat::Lenval, TStringBuf("A")), TString(
        "\x01"
        "A",
        2));
    EXPECT_EQ(NFraming::PackToString(NFraming::EFormat::LightProtoseq, TStringBuf("A")), TString(
        "\x01\x00\x00\x00"
        "A",
        5));
}

TEST(TFramingWireCanonTest, PackedBytesMatchFrozenCorpus)
{
    auto corpus = ReadCorpus();
    EXPECT_EQ(corpus["sync_word"].GetStringSafe(), SourceSyncWordHex);
    EXPECT_EQ(ToHex(NFraming::NPrivate::SyncWord), SourceSyncWordHex);

    std::vector<TString> payloads;
    for (const auto& payloadHex : corpus["payloads"].GetArraySafe()) {
        payloads.push_back(HexDecode(payloadHex.GetStringSafe()));
    }
    ASSERT_EQ(payloads.size(), 3u);
    EXPECT_EQ(payloads[0], "alpha");
    EXPECT_EQ(payloads[1], "");
    EXPECT_EQ(payloads[2], TString("\x00\xff\x01", 3));

    auto messages = MakeOutputMessages(payloads);
    for (auto format : Formats) {
        auto formatName = FormatEnum(format);
        const auto& frames = corpus["frames"][formatName].GetArraySafe();
        ASSERT_EQ(frames.size(), payloads.size()) << formatName;
        for (int index = 0; index < std::ssize(payloads); ++index) {
            EXPECT_EQ(
                    ToHex(NFraming::PackToString(static_cast<NFraming::EFormat>(format), TStringBuf(payloads[index]))),
                    frames[index].GetStringSafe())
                << formatName << " " << index;
        }
        EXPECT_EQ(ToHex(PackBatch(messages, "data", format)), corpus["pack_batch"][formatName].GetStringSafe())
            << formatName;
    }
}

TEST(TFramingWireCanonTest, AutoPacksAsProtoseq)
{
    auto messages = MakeOutputMessages(MakeProtoPayloads(3));
    EXPECT_EQ(PackBatch(messages, "data", EFramingFormat::Auto), PackBatch(messages, "data", EFramingFormat::Protoseq));
}

TEST(TFramingWireCanonTest, ExplicitFormatsRoundTripInOrder)
{
    auto payloads = MakeProtoPayloads(5);
    auto messages = MakeOutputMessages(payloads);
    for (auto format : {EFramingFormat::Protoseq, EFramingFormat::Lenval, EFramingFormat::LightProtoseq}) {
        auto packed = PackBatch(messages, "data", format);
        auto unpacked = UnpackBatch<NProto::TExampleMessage>(packed, format);
        ASSERT_TRUE(unpacked.IsOK()) << FormatEnum(format) << " " << ToString(unpacked);
        EXPECT_EQ(SerializeAll(unpacked.Value()), payloads) << FormatEnum(format);
    }
}

TEST(TFramingWireCanonTest, AutoDecodesOnlyProtoseqAndLenval)
{
    auto payloads = MakeProtoPayloads(5);
    auto messages = MakeOutputMessages(payloads);
    for (auto format : {EFramingFormat::Protoseq, EFramingFormat::Lenval}) {
        auto packed = PackBatch(messages, "data", format);
        auto unpacked = UnpackBatch<NProto::TExampleMessage>(packed, EFramingFormat::Auto);
        ASSERT_TRUE(unpacked.IsOK()) << FormatEnum(format);
        EXPECT_EQ(SerializeAll(unpacked.Value()), payloads) << FormatEnum(format);
    }

    auto lightProtoseqPacked = PackBatch(messages, "data", EFramingFormat::LightProtoseq);
    auto lightProtoseq = UnpackBatch<NProto::TExampleMessage>(lightProtoseqPacked, EFramingFormat::Auto);
    EXPECT_TRUE(!lightProtoseq.IsOK() || SerializeAll(lightProtoseq.Value()) != payloads);
}

TEST(TFramingWireCanonTest, EmptyInputYieldsNoMessages)
{
    for (auto format : Formats) {
        auto unpacked = UnpackBatch<NProto::TExampleMessage>(TStringBuf(), format);
        ASSERT_TRUE(unpacked.IsOK()) << FormatEnum(format);
        EXPECT_TRUE(unpacked.Value().empty()) << FormatEnum(format);
    }
    EXPECT_EQ(PackBatch({}, "data", EFramingFormat::Protoseq), "");
}

TEST(TFramingWireCanonTest, SkippedBytesArePermanentErrors)
{
    auto messages = MakeOutputMessages(MakeProtoPayloads(2));
    auto packed = "garbage" + PackBatch(messages, "data", EFramingFormat::Protoseq);
    auto unpacked = UnpackBatch<NProto::TExampleMessage>(packed, EFramingFormat::Protoseq);
    ASSERT_FALSE(unpacked.IsOK());
    EXPECT_TRUE(unpacked.Attributes().Get<bool>("is_permanent", false) ||
        unpacked.InnerErrors().at(0).Attributes().Get<bool>("is_permanent", false));
}

TEST(TFramingWireCanonTest, TruncatedFramesAreErrors)
{
    auto messages = MakeOutputMessages(MakeProtoPayloads(3));
    for (auto format : {EFramingFormat::Protoseq, EFramingFormat::Lenval, EFramingFormat::LightProtoseq}) {
        auto packed = PackBatch(messages, "data", format);
        packed.resize(packed.size() - 3);
        EXPECT_FALSE(UnpackBatch<NProto::TExampleMessage>(packed, format).IsOK()) << FormatEnum(format);
    }
}

TEST(TFramingWireCanonTest, UnpackedMessagesOutliveInputBuffer)
{
    auto payloads = MakeProtoPayloads(4);
    std::vector<NProto::TExampleMessage> unpacked;
    {
        auto packed = PackBatch(MakeOutputMessages(payloads), "data", EFramingFormat::Lenval);
        unpacked = UnpackBatch<NProto::TExampleMessage>(packed, EFramingFormat::Lenval).ValueOrThrow();
        std::fill(packed.begin(), packed.end(), '\0');
    }
    EXPECT_EQ(SerializeAll(unpacked), payloads);
}

TEST(TFramingWireCanonTest, PackBatchesHonorsRowAndByteLimits)
{
    auto payloads = MakeProtoPayloads(5);
    auto messages = MakeOutputMessages(payloads);
    std::deque<TOutputMessageConstPtr> queue(messages.begin(), messages.end());

    auto byRows = PackBatches(queue, "data", EFramingFormat::Lenval, /*maxRowsPerMessage*/ 2, /*maxBytesPerMessage*/ 1 << 20);
    ASSERT_EQ(byRows.size(), 3u);
    std::vector<size_t> sizes;
    std::vector<TString> roundTrip;
    for (const auto& [data, batch] : byRows) {
        sizes.push_back(batch.size());
        EXPECT_EQ(data, PackBatch(batch, "data", EFramingFormat::Lenval));
        auto unpacked = UnpackBatch<NProto::TExampleMessage>(data, EFramingFormat::Lenval).ValueOrThrow();
        for (const auto& payload : SerializeAll(unpacked)) {
            roundTrip.push_back(payload);
        }
    }
    EXPECT_EQ(sizes, (std::vector<size_t>{2, 2, 1}));
    EXPECT_EQ(roundTrip, payloads);

    auto byBytes = PackBatches(queue, "data", EFramingFormat::Lenval, /*maxRowsPerMessage*/ 1000, /*maxBytesPerMessage*/ 1);
    ASSERT_EQ(byBytes.size(), payloads.size());
    for (int index = 0; index < std::ssize(byBytes); ++index) {
        EXPECT_EQ(byBytes[index].second, std::vector<TOutputMessageConstPtr>{messages[index]});
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
