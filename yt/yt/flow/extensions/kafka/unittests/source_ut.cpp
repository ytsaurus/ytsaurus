#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/extensions/kafka/source.h>

#include <yt/yt/flow/library/cpp/common/key.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TKafkaKeyTest, RoundTrip)
{
    for (int partitionIndex : {0, 1, 7, 128, 100500}) {
        auto key = GenerateKafkaKey("some-identity", partitionIndex);
        EXPECT_EQ(ExtractKafkaPartitionIndex(key), partitionIndex);
    }
}

TEST(TKafkaKeyTest, DistinctIdentitiesProduceDistinctKeys)
{
    EXPECT_NE(GenerateKafkaKey("identity-a", 0), GenerateKafkaKey("identity-b", 0));
    EXPECT_NE(GenerateKafkaKey("identity", 0), GenerateKafkaKey("identity", 1));
}

TEST(TKafkaKeyTest, ExtractRejectsWrongShape)
{
    auto malformedKey = MakeKey(TStringBuf("only-one-column"));
    EXPECT_THROW(ExtractKafkaPartitionIndex(malformedKey), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TKafkaSchemaTest, HasExpectedColumns)
{
    using NTableClient::EValueType;

    auto schema = GetKafkaSourceSchema();

    std::vector<std::pair<std::string, EValueType>> expectedColumns{
        {"data", EValueType::String},
        {"key", EValueType::String},
        {"unparsed", EValueType::Any},
        {"partition", EValueType::Int64},
        {"offset", EValueType::Int64},
        {"sub_offset", EValueType::Int64},
        {"timestamp", EValueType::Int64},
    };

    EXPECT_EQ(schema->GetColumnCount(), std::ssize(expectedColumns));

    for (const auto& [name, type] : expectedColumns) {
        const auto* column = schema->FindColumn(name);
        ASSERT_NE(column, nullptr) << "missing column " << name;
        EXPECT_EQ(column->GetWireType(), type) << "wrong type for column " << name;
    }
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TSharedRef> MakeFrames(const std::vector<std::string>& payloads)
{
    std::vector<TSharedRef> frames;
    for (const auto& payload : payloads) {
        frames.push_back(TSharedRef::FromString(payload));
    }
    return frames;
}

TEST(TKafkaExpandedSizeTest, ChargesEveryRowTheKeyAndTheRowItself)
{
    auto frames = MakeFrames({"first", "second"});

    // "first" and "second" are 11 bytes of payload; each row is charged the row itself on top.
    EXPECT_EQ(GetKafkaExpandedSize(frames, 0), 11 + 2 * GetKafkaRowOverhead());
    // And the key once per row, not once per record.
    EXPECT_EQ(GetKafkaExpandedSize(frames, 100), 11 + 2 * GetKafkaRowOverhead() + 2 * 100);
}

TEST(TKafkaExpandedSizeTest, ChargesOneRowWithoutOutputBytes)
{
    EXPECT_EQ(GetKafkaExpandedSize(1, 0, 0), GetKafkaRowOverhead());
    EXPECT_EQ(GetKafkaExpandedSize(1, 0, 4096), GetKafkaRowOverhead() + 4096);
    EXPECT_EQ(GetKafkaExpandedSize(1, 123, 4096), GetKafkaRowOverhead() + 123 + 4096);
}

TEST(TKafkaExpandedSizeTest, ChargesRowsThatCarryNoBytes)
{
    // Empty rows under no key: no bytes of payload, a full row each in memory.
    auto frames = MakeFrames(std::vector<std::string>(1000));

    EXPECT_EQ(GetKafkaExpandedSize(frames, 0), 1000 * GetKafkaRowOverhead());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
