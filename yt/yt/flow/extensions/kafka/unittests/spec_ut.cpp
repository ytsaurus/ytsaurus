#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/extensions/kafka/kafka_client.h>
#include <yt/yt/flow/extensions/kafka/source.h>
#include <yt/yt/flow/extensions/kafka/spec.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <class TParameters>
TIntrusivePtr<TParameters> Parse(const char* spec)
{
    return NYTree::ConvertTo<TIntrusivePtr<TParameters>>(NYson::TYsonStringBuf(TStringBuf(spec)));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TKafkaInfoSpecTest, RequiresANonEmptyTopic)
{
    EXPECT_THROW(Parse<TKafkaSinkParameters>("{}"), std::exception);
    EXPECT_THROW(Parse<TKafkaSinkParameters>("{topic=\"\"}"), std::exception);
    EXPECT_NO_THROW(Parse<TKafkaSinkParameters>("{topic=t}"));
}

TEST(TKafkaInfoSpecTest, HasDocumentedDefaults)
{
    // The broker-facing periods are source-side parameters; sinks take only the topic.
    auto parameters = Parse<TKafkaSourceParameters>("{topic=t;group_id=g}");

    EXPECT_EQ(parameters->UpdatePartitionCountPeriod, TDuration::Seconds(60));
    EXPECT_EQ(parameters->MetadataTimeout, TDuration::Seconds(30));
}

TEST(TKafkaInfoSpecTest, RejectsNonPositivePeriods)
{
    EXPECT_THROW(Parse<TKafkaSourceParameters>("{topic=t;group_id=g;metadata_timeout=0}"), std::exception);
    EXPECT_THROW(
        Parse<TKafkaSourceParameters>("{topic=t;group_id=g;update_partition_count_period=0}"),
        std::exception);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TKafkaSourceSpecTest, RequiresAGroupId)
{
    EXPECT_THROW(Parse<TKafkaSourceParameters>("{topic=t}"), std::exception);
    EXPECT_THROW(Parse<TKafkaSourceParameters>("{topic=t;group_id=\"\"}"), std::exception);
    EXPECT_NO_THROW(Parse<TKafkaSourceParameters>("{topic=t;group_id=g}"));
}

TEST(TKafkaSourceSpecTest, HasDocumentedDefaults)
{
    auto parameters = Parse<TKafkaSourceParameters>("{topic=t;group_id=g}");

    EXPECT_EQ(parameters->MaxBufferBytes, 64ll * 1024 * 1024);
    EXPECT_FALSE(parameters->PartitionFilter.has_value());
    EXPECT_TRUE(parameters->UseConsumerGroupOffset);
}

TEST(TKafkaSourceSpecTest, AcceptsTheConsumerGroupOffsetOptOut)
{
    auto parameters = Parse<TKafkaSourceParameters>("{topic=t;group_id=g;use_consumer_group_offset=%false}");

    EXPECT_FALSE(parameters->UseConsumerGroupOffset);
}

TEST(TKafkaSourceSpecTest, RejectsANonPositiveBufferCap)
{
    EXPECT_THROW(Parse<TKafkaSourceParameters>("{topic=t;group_id=g;max_buffer_bytes=0}"), std::exception);
}

TEST(TKafkaSourceSpecTest, ValidatesPartitionFilterIntervals)
{
    EXPECT_NO_THROW(Parse<TKafkaSourceParameters>("{topic=t;group_id=g;partition_filter=[[0;2];[5;7]]}"));
    // Inverted, empty and negative ranges silently select nothing — reject them up front, like the
    // sibling connectors do.
    EXPECT_THROW_WITH_SUBSTRING(
        Parse<TKafkaSourceParameters>("{topic=t;group_id=g;partition_filter=[[3;1]]}"),
        "partition_filter");
    EXPECT_THROW_WITH_SUBSTRING(
        Parse<TKafkaSourceParameters>("{topic=t;group_id=g;partition_filter=[[2;2]]}"),
        "partition_filter");
    EXPECT_THROW_WITH_SUBSTRING(
        Parse<TKafkaSourceParameters>("{topic=t;group_id=g;partition_filter=[[-1;2]]}"),
        "partition_filter");
}

TEST(TDynamicKafkaSourceSpecTest, HasDocumentedDefaults)
{
    auto parameters = Parse<TDynamicKafkaSourceParameters>("{}");

    EXPECT_EQ(parameters->PollTimeout, TDuration::MilliSeconds(200));
    EXPECT_EQ(parameters->WatermarkUpdatePeriod, TDuration::Seconds(5));
    EXPECT_EQ(parameters->MalformedMessagePolicy, EMalformedKafkaMessagePolicy::Fail);
}

TEST(TDynamicKafkaSourceSpecTest, RejectsNonPositiveTimeouts)
{
    // A zero poll timeout would turn the poll loop into a non-blocking spin.
    EXPECT_THROW(Parse<TDynamicKafkaSourceParameters>("{poll_timeout=0}"), std::exception);
    EXPECT_THROW(Parse<TDynamicKafkaSourceParameters>("{watermark_update_period=0}"), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TKafkaClientConfigTest, RequiresBrokers)
{
    EXPECT_THROW(Parse<TKafkaClientConfig>("{}"), std::exception);
    EXPECT_THROW(Parse<TKafkaClientConfig>("{bootstrap_servers=\"\"}"), std::exception);
    EXPECT_NO_THROW(Parse<TKafkaClientConfig>("{bootstrap_servers=\"broker:9092\"}"));
}

TEST(TKafkaClientConfigTest, RejectsReservedExtraConfigKeys)
{
    // Keys the connector owns are applied after extra_config, so a value here would be silently
    // overridden.
    EXPECT_NO_THROW(Parse<TKafkaClientConfig>(
        "{bootstrap_servers=b;extra_config={\"message.max.bytes\"=\"1000000\"}}"));
    EXPECT_THROW_WITH_SUBSTRING(
        Parse<TKafkaClientConfig>("{bootstrap_servers=b;extra_config={\"auto.offset.reset\"=error}}"),
        "managed by the connector");
    EXPECT_THROW_WITH_SUBSTRING(
        Parse<TKafkaClientConfig>("{bootstrap_servers=b;extra_config={\"enable.idempotence\"=\"false\"}}"),
        "managed by the connector");
    // librdkafka canonical spellings of keys whose aliases the connector sets: letting them through
    // would repoint the cluster or auth while the source identity keeps the declared bootstrap string.
    EXPECT_THROW_WITH_SUBSTRING(
        Parse<TKafkaClientConfig>("{bootstrap_servers=b;extra_config={\"metadata.broker.list\"=\"other:9092\"}}"),
        "managed by the connector");
    EXPECT_THROW_WITH_SUBSTRING(
        Parse<TKafkaClientConfig>("{bootstrap_servers=b;extra_config={\"sasl.mechanisms\"=PLAIN}}"),
        "managed by the connector");
    // librdkafka defaults this to true for producers; the info controller turns it off.
    EXPECT_THROW_WITH_SUBSTRING(
        Parse<TKafkaClientConfig>("{bootstrap_servers=b;extra_config={\"allow.auto.create.topics\"=\"true\"}}"),
        "managed by the connector");
    // The sink acknowledges writes only from delivery reports; suppressing the successful ones would
    // leave every write pending forever.
    EXPECT_THROW_WITH_SUBSTRING(
        Parse<TKafkaClientConfig>("{bootstrap_servers=b;extra_config={\"delivery.report.only.error\"=\"true\"}}"),
        "managed by the connector");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TKafkaSinkSpecTest, HasDocumentedDefaults)
{
    auto parameters = Parse<TKafkaSinkParameters>("{topic=t}");

    EXPECT_EQ(parameters->PayloadColumn, "data");
    EXPECT_TRUE(parameters->KeyColumn.empty());
    EXPECT_TRUE(parameters->MessageIdHeader.empty());
}

TEST(TKafkaSinkSpecTest, AcceptsTheMessageIdHeader)
{
    auto parameters = Parse<TKafkaSinkParameters>("{topic=t;message_id_header=flow_message_id}");
    EXPECT_EQ(parameters->MessageIdHeader, "flow_message_id");

    auto atLeastOnce = Parse<TAtLeastOnceKafkaSinkParameters>("{topic=t;message_id_header=flow_message_id}");
    EXPECT_EQ(atLeastOnce->MessageIdHeader, "flow_message_id");
}

TEST(TKafkaSinkSpecTest, PerMessageSinksAcceptAKeyColumn)
{
    EXPECT_EQ(Parse<TKafkaSinkParameters>("{topic=t;key_column=key}")->KeyColumn, "key");
    EXPECT_EQ(Parse<TAtLeastOnceKafkaSinkParameters>("{topic=t;key_column=key}")->KeyColumn, "key");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TKafkaPartitionFilterTest, KeepsEveryPartitionWithoutAFilter)
{
    EXPECT_EQ(SelectKafkaPartitions(4, std::nullopt), (std::vector{0, 1, 2, 3}));
    EXPECT_TRUE(SelectKafkaPartitions(0, std::nullopt).empty());
}

TEST(TKafkaPartitionFilterTest, TreatsRangesAsHalfOpen)
{
    std::vector<std::pair<int, int>> filter{{1, 3}};
    EXPECT_EQ(SelectKafkaPartitions(5, filter), (std::vector{1, 2}));
}

TEST(TKafkaPartitionFilterTest, UnionsSeveralRanges)
{
    std::vector<std::pair<int, int>> filter{{0, 1}, {3, 5}};
    EXPECT_EQ(SelectKafkaPartitions(6, filter), (std::vector{0, 3, 4}));
}

TEST(TKafkaPartitionFilterTest, ToleratesOverlappingAndEmptyRanges)
{
    std::vector<std::pair<int, int>> filter{{0, 3}, {2, 4}, {7, 7}};
    EXPECT_EQ(SelectKafkaPartitions(6, filter), (std::vector{0, 1, 2, 3}));
}

TEST(TKafkaPartitionFilterTest, ClampsToTheActualPartitionCount)
{
    std::vector<std::pair<int, int>> filter{{2, 100}};
    EXPECT_EQ(SelectKafkaPartitions(4, filter), (std::vector{2, 3}));
}

TEST(TKafkaPartitionFilterTest, KeepsNothingWhenNoRangeMatches)
{
    std::vector<std::pair<int, int>> filter{{10, 20}};
    EXPECT_TRUE(SelectKafkaPartitions(4, filter).empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
