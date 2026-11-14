#include <yt/yql/providers/dq/metrics/dynamic_counters.h>

#include <yt/yt/library/profiling/solomon/registry.h>

#include <library/cpp/monlib/metrics/histogram_collector.h>

#include <gtest/gtest.h>

#include <map>

namespace NYql {
namespace {

using namespace NYT;
using namespace NYT::NProfiling;

TEST(TDynamicCountersProducerTest, ValuesAndTags)
{
    auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
    auto scheduler = root->GetSubgroup("component", "scheduler");
    auto counter = scheduler->GetCounter("Scheduled", true);
    auto gauge = scheduler->GetCounter("Queued");
    *counter = 42;
    *gauge = -3;
    auto producer = CreateDynamicCountersProducer(root);

    auto buffer = producer->GetBuffer();
    ASSERT_EQ(buffer->GetCounters().size(), 1u);
    ASSERT_EQ(buffer->GetGauges().size(), 1u);
    EXPECT_EQ(buffer->GetCounters()[0], std::make_tuple(
        std::string("/Scheduled"), TTagList({{"component", "scheduler"}}), i64(42)));
    EXPECT_EQ(buffer->GetGauges()[0], std::make_tuple(
        std::string("/Queued"), TTagList({{"component", "scheduler"}}), double(-3)));

    *counter += 5;
    *gauge = 8;
    buffer = producer->GetBuffer();
    EXPECT_EQ(std::get<2>(buffer->GetCounters()[0]), 47);
    EXPECT_EQ(std::get<2>(buffer->GetGauges()[0]), 8);
    EXPECT_EQ(counter->Val(), 47);
}

TEST(TDynamicCountersProducerTest, NestedAndCustomLabels)
{
    auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
    auto group = root->GetSubgroup("component", "outer")
        ->GetSubgroup("component", "inner")
        ->GetSubgroup("counter_label", "group");
    *group->GetNamedCounter("name", "Value") = 7;
    root->GetCounter("Private", false, NMonitoring::TCountableBase::EVisibility::Private);
    auto buffer = CreateDynamicCountersProducer(root)->GetBuffer();
    ASSERT_EQ(buffer->GetGauges().size(), 1u);
    EXPECT_EQ(buffer->GetGauges()[0], std::make_tuple(
        std::string("/Value"), TTagList({
            {"component", "outer"}, {"component_2", "inner"},
            {"counter_label", "group"}, {"counter_label_2", "name"}}), double(7)));
}

TEST(TDynamicCountersProducerTest, HistogramBuckets)
{
    auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
    auto histogram = root->GetHistogram("Latency", NMonitoring::ExplicitHistogram({1, 10}));
    histogram->Collect(0.0, 2);
    histogram->Collect(5.0, 3);
    histogram->Collect(11.0, 4);
    auto gauge = root->GetHistogram("Current", NMonitoring::ExplicitHistogram({1}), false);
    gauge->Collect(0.0, 5);

    auto buffer = CreateDynamicCountersProducer(root)->GetBuffer();
    ASSERT_EQ(buffer->GetCounters().size(), 3u);
    ASSERT_EQ(buffer->GetGauges().size(), 2u);
    const std::vector<std::string> bounds = {"1", "10", "+Inf"};
    for (size_t i = 0; i < bounds.size(); ++i) {
        EXPECT_EQ(buffer->GetCounters()[i], std::make_tuple(
            std::string("/Latency/bucket"), TTagList({{"upper_bound", bounds[i]}}), i64(i + 2)));
    }
    EXPECT_EQ(std::get<2>(buffer->GetGauges()[0]), 5);
    EXPECT_EQ(std::get<2>(buffer->GetGauges()[1]), 0);
}

class TMetricConsumer final : public NMonitoring::IMetricConsumer {
public:
    void OnStreamBegin() override { }
    void OnStreamEnd() override { }
    void OnCommonTime(TInstant) override { }
    void OnMetricBegin(NMonitoring::EMetricType) override { }
    void OnMetricEnd() override { }
    void OnLabelsBegin() override { Labels.clear(); }
    void OnLabelsEnd() override { }
    void OnLabel(TStringBuf key, TStringBuf value) override {
        Labels.emplace(std::string(key), std::string(value));
    }
    void OnLabel(ui32 key, ui32 value) override { OnLabel(LabelCache[key], LabelCache[value]); }
    std::pair<ui32, ui32> PrepareLabel(TStringBuf key, TStringBuf value) override {
        LabelCache.emplace_back(key);
        LabelCache.emplace_back(value);
        return {LabelCache.size() - 2, LabelCache.size() - 1};
    }
    void OnDouble(TInstant, double value) override { Gauges[Labels] = value; }
    void OnInt64(TInstant, i64 value) override { Counters[Labels] = value; }
    void OnUint64(TInstant, ui64) override { }
    void OnHistogram(TInstant, NMonitoring::IHistogramSnapshotPtr) override { }
    void OnLogHistogram(TInstant, NMonitoring::TLogHistogramSnapshotPtr) override { }
    void OnSummaryDouble(TInstant, NMonitoring::ISummaryDoubleSnapshotPtr) override { }

    using TLabels = std::map<std::string, std::string>;
    TLabels Labels;
    std::vector<TString> LabelCache;
    std::map<TLabels, double> Gauges;
    std::map<TLabels, i64> Counters;
};

TMetricConsumer Collect(const TSolomonRegistryPtr& registry)
{
    registry->ProcessRegistrations();
    auto iteration = registry->GetNextIteration();
    registry->Collect();
    TReadOptions options;
    options.Times = {{{registry->IndexOf(iteration)}, TInstant::Now()}};
    TMetricConsumer consumer;
    registry->ReadSensors(options, &consumer);
    return consumer;
}

TEST(TDynamicCountersProducerTest, RegistryIntegrationAndGroupRemoval)
{
    auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
    auto producer = CreateDynamicCountersProducer(root);
    auto registry = New<TSolomonRegistry>();
    registry->SetWindowSize(12);
    TProfiler(registry, "/yql_agent/dq")
        .WithProjectionsDisabled()
        .WithProducerRemoveSupport()
        .AddProducer("", producer);

    auto sensors = Collect(registry);
    EXPECT_TRUE(sensors.Gauges.empty());
    EXPECT_TRUE(sensors.Counters.empty());

    const TMetricConsumer::TLabels gaugeLabels = {
        {"sensor", "yt.yql_agent.dq.Queued"}, {"user", "alice"}};
    const TMetricConsumer::TLabels counterLabels = {
        {"sensor", "yt.yql_agent.dq.Scheduled"}, {"user", "alice"}};
    auto addGroup = [&] (i64 gauge, i64 counter) {
        auto group = root->GetSubgroup("user", "alice");
        *group->GetCounter("Queued") = gauge;
        *group->GetCounter("Scheduled", true) = counter;
    };
    addGroup(3, 10);
    sensors = Collect(registry);
    ASSERT_EQ(sensors.Gauges.size(), 1u);
    ASSERT_EQ(sensors.Counters.size(), 1u);
    EXPECT_EQ(sensors.Gauges.at(gaugeLabels), 3);
    EXPECT_EQ(sensors.Counters.at(counterLabels), 10);

    addGroup(7, 15);
    sensors = Collect(registry);
    EXPECT_EQ(sensors.Gauges.at(gaugeLabels), 7);
    EXPECT_EQ(sensors.Counters.at(counterLabels), 15);

    root->RemoveSubgroup("user", "alice");
    sensors = Collect(registry);
    EXPECT_TRUE(sensors.Gauges.empty());
    EXPECT_TRUE(sensors.Counters.empty());

    addGroup(2, 1);
    sensors = Collect(registry);
    EXPECT_EQ(sensors.Gauges.at(gaugeLabels), 2);
    EXPECT_EQ(sensors.Counters.at(counterLabels), 1);
}

} // namespace
} // namespace NYql
