#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/server/output_collector.h>

#include <yt/yt/flow/library/cpp/common/key.h>

#include <yt/yt/flow/library/cpp/process_function/testing/entity_builders.h>

namespace NYT::NFlow::NCompanionServer {
namespace {

////////////////////////////////////////////////////////////////////////////////

TMessage MakeOutputMessage(ui64 value)
{
    return NTesting::MakeTestRawMessage(
        TStreamId("output"),
        NTesting::DefaultTestKeySchema(),
        [&] (TMessageBuilder& builder) {
            builder.Payload().Set(value, "key");
        });
}

TEST(TGroupingOutputCollectorTest, GroupsByParents)
{
    auto schema = NTesting::DefaultTestKeySchema();
    auto key = MakeKey(ui64{1});
    auto message = NTesting::MakeTestMessage(TStreamId("input"), key, schema);
    auto timer = NTesting::MakeTestTimer(key, TSystemTimestamp(100));

    auto root = TGroupingOutputCollector::CreateRoot(
        {message->MessageId, timer->MessageId},
        {message->Key, timer->Key});

    {
        auto child = root->SetParents({message}, {}, {});
        child->AddMessage(MakeOutputMessage(1));
        child->AddMessage(MakeOutputMessage(2), /*distribute*/ false);
    }
    {
        auto child = root->SetParents({}, {timer}, {});
        child->AddTimer(TSystemTimestamp(200), TSystemTimestamp(150));
    }
    {
        // A group with no output is dropped.
        auto child = root->SetParents({message}, {timer}, {});
        Y_UNUSED(child);
    }

    auto groups = root->TakeGroups();
    ASSERT_EQ(std::ssize(groups), 2);

    EXPECT_EQ(groups[0].ParentIds, std::vector<TMessageId>{message->MessageId});
    ASSERT_EQ(std::ssize(groups[0].Messages), 2);
    EXPECT_EQ(groups[0].Distribute, (std::vector<bool>{true, false}));

    EXPECT_EQ(groups[1].ParentIds, std::vector<TMessageId>{timer->MessageId});
    ASSERT_EQ(std::ssize(groups[1].Timers), 1);
    EXPECT_EQ(groups[1].Timers[0].TriggerTimestamp, TSystemTimestamp(200));
    EXPECT_EQ(groups[1].Timers[0].EventTimestamp, TSystemTimestamp(150));
    EXPECT_FALSE(groups[1].Timers[0].StreamId.has_value());
}

TEST(TGroupingOutputCollectorTest, RootOutputAttributedToWholeBatch)
{
    auto root = TGroupingOutputCollector::CreateRoot(
        {TMessageId("m1"), TMessageId("m2")},
        {MakeKey(ui64{1}), MakeKey(ui64{2})});
    root->AddMessage(MakeOutputMessage(1));

    auto groups = root->TakeGroups();
    ASSERT_EQ(std::ssize(groups), 1);
    EXPECT_EQ(groups[0].ParentIds, (std::vector<TMessageId>{TMessageId("m1"), TMessageId("m2")}));
    EXPECT_EQ(std::ssize(groups[0].Messages), 1);
}

TEST(TGroupingOutputCollectorTest, ImplicitTimerRequiresSingleParentKey)
{
    auto mixedKeys = TGroupingOutputCollector::CreateRoot(
        {TMessageId("m1"), TMessageId("m2")},
        {MakeKey(ui64{1}), MakeKey(ui64{2})});
    EXPECT_THROW_WITH_SUBSTRING(
        mixedKeys->AddTimer(TSystemTimestamp(300)),
        "same key");

    auto noParents = TGroupingOutputCollector::CreateRoot({}, {});
    EXPECT_THROW_WITH_SUBSTRING(
        noParents->AddTimer(TStreamId("timers"), TSystemTimestamp(300)),
        "require parent entities");
}

TEST(TGroupingOutputCollectorTest, TimerObjectConversion)
{
    auto root = TGroupingOutputCollector::CreateRoot({TMessageId("m1")}, {MakeKey(ui64{1})});

    TTimer timer;
    timer.Key = MakeKey(ui64{1});
    timer.KeySchema = NTesting::DefaultTestKeySchema();
    timer.StreamId = TStreamId("timers");
    timer.TriggerTimestamp = TSystemTimestamp(500);
    timer.EventTimestamp = TSystemTimestamp(400);
    root->AddTimer(std::move(timer));

    auto groups = root->TakeGroups();
    ASSERT_EQ(std::ssize(groups), 1);
    ASSERT_EQ(std::ssize(groups[0].Timers), 1);
    EXPECT_EQ(groups[0].Timers[0].TriggerTimestamp, TSystemTimestamp(500));
    EXPECT_EQ(groups[0].Timers[0].EventTimestamp, TSystemTimestamp(400));
    EXPECT_EQ(groups[0].Timers[0].StreamId, TStreamId("timers"));
}

TEST(TGroupingOutputCollectorTest, ForeignKeyTimerThrows)
{
    auto root = TGroupingOutputCollector::CreateRoot({TMessageId("m1")}, {MakeKey(ui64{1})});

    TTimer timer;
    timer.Key = MakeKey(ui64{2});
    timer.KeySchema = NTesting::DefaultTestKeySchema();
    timer.TriggerTimestamp = TSystemTimestamp(500);
    EXPECT_THROW_WITH_SUBSTRING(
        root->AddTimer(std::move(timer)),
        "must target the key of a group parent");
}

TEST(TGroupingOutputCollectorTest, KeyedTimerRoutedToSameKeyParents)
{
    auto root = TGroupingOutputCollector::CreateRoot(
        {TMessageId("m1"), TMessageId("m2"), TMessageId("m3")},
        {MakeKey(ui64{1}), MakeKey(ui64{2}), MakeKey(ui64{2})});

    // A timer keyed to one key of a multi-key batch lands in a group holding
    // only the same-key parents (the wire keys timers by group parents).
    for (auto trigger : {500, 600}) {
        TTimer timer;
        timer.Key = MakeKey(ui64{2});
        timer.KeySchema = NTesting::DefaultTestKeySchema();
        timer.TriggerTimestamp = TSystemTimestamp(trigger);
        root->AddTimer(std::move(timer));
    }

    auto groups = root->TakeGroups();
    ASSERT_EQ(std::ssize(groups), 1);
    EXPECT_EQ(groups[0].ParentIds, (std::vector<TMessageId>{TMessageId("m2"), TMessageId("m3")}));
    ASSERT_EQ(std::ssize(groups[0].Timers), 2);
    EXPECT_EQ(groups[0].Timers[0].TriggerTimestamp, TSystemTimestamp(500));
    EXPECT_EQ(groups[0].Timers[1].TriggerTimestamp, TSystemTimestamp(600));
}

TEST(TGroupingOutputCollectorTest, KeylessTimerObjectRequiresSingleParentKey)
{
    auto root = TGroupingOutputCollector::CreateRoot(
        {TMessageId("m1"), TMessageId("m2")},
        {MakeKey(ui64{1}), MakeKey(ui64{2})});

    TTimer timer;
    timer.TriggerTimestamp = TSystemTimestamp(500);
    EXPECT_THROW_WITH_SUBSTRING(
        root->AddTimer(std::move(timer)),
        "same key");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanionServer
