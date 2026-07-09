#include <random>

#include <yt/yt/flow/library/cpp/common/inflight_tracker.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>
#include <yt/yt/flow/library/cpp/common/timer.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/test_framework/framework.h>

#include <util/generic/map.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TComputationStreamSpecStoragePtr MakeSpecStorage(
    const TStreamId& streamId,
    const NTableClient::TTableSchemaPtr& schema)
{
    auto streamSpec = New<TStreamSpec>();
    streamSpec->Schema = schema;
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> specs;
    specs[streamId][TStreamSpecId(1)] = streamSpec;
    auto streamSpecs = New<TStreamSpecs>(specs);
    return New<TComputationStreamSpecStorage>(
        std::move(streamSpecs),
        /*groupBySchema*/ New<NTableClient::TTableSchema>(),
        /*evaluatorCache*/ nullptr);
}

TInputTimerConstPtr MakeInputTimer(
    const std::string& streamId,
    const std::string& id,
    ui64 systemTimestamp,
    ui64 eventTimestamp)
{
    static const auto KeySchema = New<NTableClient::TTableSchema>();
    TTimer timer;
    timer.MessageId = TMessageId(id);
    timer.StreamId = TStreamId(streamId);
    timer.SystemTimestamp = TSystemTimestamp(systemTimestamp);
    timer.EventTimestamp = TSystemTimestamp(eventTimestamp);
    timer.AlignmentTimestamp = TSystemTimestamp(systemTimestamp);
    timer.TriggerTimestamp = TSystemTimestamp(systemTimestamp);
    timer.Key = MakeKey();
    timer.KeySchema = KeySchema;
    return New<TInputTimer>(std::move(timer), KeySchema);
}

TOutputMessageConstPtr MakeOutputMessage(
    const TStreamId& streamId,
    const NTableClient::TTableSchemaPtr& schema,
    const std::string& id,
    ui64 systemTimestamp,
    ui64 eventTimestamp)
{
    TMessageBuilder builder(streamId, schema);
    builder.SetMessageId(TMessageId(id));
    builder.SetSystemTimestamp(TSystemTimestamp(systemTimestamp));
    builder.SetEventTimestamp(TSystemTimestamp(eventTimestamp));
    builder.SetAlignmentTimestamp(TSystemTimestamp(systemTimestamp));
    builder.Payload().Set("payload", "payload");
    return New<TOutputMessage>(builder.Finish(), MakeSpecStorage(streamId, schema));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TTestInflightStoreTest, Basic)
{
    const auto schema = New<NTableClient::TTableSchema>(
        std::vector{
            NTableClient::TColumnSchema("payload", NTableClient::EValueType::String),
        });
    auto spec = New<TWatermarkPercentileSpec>();
    spec->Value = PreciseWatermarkPercentile;
    const auto store = New<TInflightTracker>(NProfiling::TProfiler{}, spec);

    auto createMessage = [&] (const std::string& id, ui64 systemTimestamp, ui64 eventTimestamp) {
        return MakeOutputMessage(TStreamId("stream"), schema, id, systemTimestamp, eventTimestamp);
    };

    EXPECT_EQ(store->GetCount(), 0);
    EXPECT_EQ(store->GetMinSystemTimestamp(), std::nullopt);

    EXPECT_EQ(store->TryRegister(createMessage("1", 1747052600ull, 1747052500ull)), true);
    EXPECT_EQ(store->GetCount(), 1);
    EXPECT_EQ(store->GetMinSystemTimestamp()->Underlying(), 1747052600ull);
    EXPECT_EQ(store->GetMinEventTimestamp()->Underlying(), 1747052500ull);

    EXPECT_EQ(store->TryRegister(createMessage("1", 1747052500ull, 1747052500ull)), false);
    EXPECT_EQ(store->GetCount(), 1);
    EXPECT_EQ(store->GetMinSystemTimestamp()->Underlying(), 1747052600ull);
    EXPECT_EQ(store->GetMinEventTimestamp()->Underlying(), 1747052500ull);

    EXPECT_EQ(store->TryRegister(createMessage("2", 1747052400ull, 1747052600ull)), true);
    EXPECT_EQ(store->GetCount(), 2);
    EXPECT_EQ(store->GetMinSystemTimestamp()->Underlying(), 1747052400ull);
    EXPECT_EQ(store->GetMinEventTimestamp()->Underlying(), 1747052500ull);

    EXPECT_EQ(store->TryRegister(createMessage("3", 1747052500ull, 1747052200ull)), true);
    EXPECT_EQ(store->GetCount(), 3);
    EXPECT_EQ(store->GetMinSystemTimestamp()->Underlying(), 1747052400ull);
    EXPECT_EQ(store->GetMinEventTimestamp()->Underlying(), 1747052200ull);

    EXPECT_EQ(store->TryUnregister(TMessageId("2")), true);
    EXPECT_EQ(store->GetCount(), 2);
    EXPECT_EQ(store->GetMinSystemTimestamp()->Underlying(), 1747052500ull);
    EXPECT_EQ(store->GetMinEventTimestamp()->Underlying(), 1747052200ull);

    EXPECT_EQ(store->TryUnregister(TMessageId("2")), false);
    EXPECT_EQ(store->GetCount(), 2);
    EXPECT_EQ(store->GetMinSystemTimestamp()->Underlying(), 1747052500ull);
    EXPECT_EQ(store->GetMinEventTimestamp()->Underlying(), 1747052200ull);

    EXPECT_EQ(store->TryUnregister(TMessageId("3")), true);
    EXPECT_EQ(store->GetCount(), 1);
    EXPECT_EQ(store->GetMinSystemTimestamp()->Underlying(), 1747052600ull);
    EXPECT_EQ(store->GetMinEventTimestamp()->Underlying(), 1747052500ull);

    EXPECT_EQ(store->TryUnregister(TMessageId("1")), true);
    EXPECT_EQ(store->GetCount(), 0);
    EXPECT_EQ(store->GetMinSystemTimestamp(), std::nullopt);
    EXPECT_EQ(store->GetMinEventTimestamp(), std::nullopt);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TTestInflightStoreTest, RegisteredTotalsCountSuccessfulRegistersOnly)
{
    const auto schema = New<NTableClient::TTableSchema>(
        std::vector{
            NTableClient::TColumnSchema("payload", NTableClient::EValueType::String),
        });
    auto spec = New<TWatermarkPercentileSpec>();
    spec->Value = PreciseWatermarkPercentile;
    auto state = New<TStreamLimitUsageState>(/*inflation*/ 0);
    const auto store = New<TInflightTracker>(NProfiling::TProfiler{}, spec, state);

    auto createMessage = [&] (const std::string& id) {
        return MakeOutputMessage(TStreamId("stream"), schema, id, 1747052500ull, 1747052500ull);
    };

    store->SyncCounters();
    EXPECT_EQ(state->Read().CumulativeCountIn, 0);
    EXPECT_EQ(state->Read().CumulativeByteIn, 0);

    auto first = createMessage("1");
    EXPECT_TRUE(store->TryRegister(first));
    store->SyncCounters();
    EXPECT_EQ(state->Read().CumulativeCountIn, 1);
    EXPECT_EQ(state->Read().CumulativeByteIn, first->ByteSize);

    EXPECT_FALSE(store->TryRegister(createMessage("1")));
    store->SyncCounters();
    EXPECT_EQ(state->Read().CumulativeCountIn, 1);
    EXPECT_EQ(state->Read().CumulativeByteIn, first->ByteSize);

    auto second = createMessage("2");
    EXPECT_TRUE(store->TryRegister(second));
    store->SyncCounters();
    EXPECT_EQ(state->Read().CumulativeCountIn, 2);
    EXPECT_EQ(state->Read().CumulativeByteIn, first->ByteSize + second->ByteSize);

    // Unregister leaves the lifetime totals untouched.
    EXPECT_TRUE(store->TryUnregister(TMessageId("1")));
    store->SyncCounters();
    EXPECT_EQ(state->Read().CumulativeCountIn, 2);
    EXPECT_EQ(state->Read().CumulativeByteIn, first->ByteSize + second->ByteSize);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TTestInflightStoreTest, SyncCountersPushesUsageToBoundStreamLimitUsageStates)
{
    const auto schema = New<NTableClient::TTableSchema>(
        std::vector{
            NTableClient::TColumnSchema("payload", NTableClient::EValueType::String),
        });
    auto spec = New<TWatermarkPercentileSpec>();
    spec->Value = PreciseWatermarkPercentile;
    auto streamId = TStreamId("stream");
    auto state = New<TStreamLimitUsageState>(/*inflation*/ 0);
    TStreamLimitUsageStateMap states;
    states.emplace(streamId, state);
    const auto store = New<TMultiInflightTracker>(
        NProfiling::TProfiler{},
        THashSet<TStreamId>{streamId},
        spec,
        std::move(states));

    auto message = MakeOutputMessage(streamId, schema, "1", 1747052500ull, 1747052500ull);
    store->Register(message);
    store->SyncCounters();
    auto usage = state->Read();
    EXPECT_EQ(usage.CumulativeByteIn, message->ByteSize);
    EXPECT_EQ(usage.CumulativeCountIn, 1);
    EXPECT_EQ(usage.CumulativeByteOut, 0);
    EXPECT_EQ(usage.CumulativeCountOut, 0);

    store->Unregister(TMessageMeta{.MessageId = message->MessageId, .StreamId = streamId});
    store->SyncCounters();
    usage = state->Read();
    EXPECT_EQ(usage.CumulativeByteIn, message->ByteSize);
    EXPECT_EQ(usage.CumulativeCountIn, 1);
    EXPECT_EQ(usage.CumulativeByteOut, message->ByteSize);
    EXPECT_EQ(usage.CumulativeCountOut, 1);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TTestInflightStoreTest, Percentile)
{
    const auto schema = New<NTableClient::TTableSchema>(
        std::vector{
            NTableClient::TColumnSchema("payload", NTableClient::EValueType::String),
        });
    THashMap<ui64, TInflightTrackerPtr> stores;
    for (const auto& percentile : std::vector<ui64>{0, 20, 40, 60, 80, 100}) {
        auto spec = New<TWatermarkPercentileSpec>();
        spec->Value = TWatermarkPercentile(percentile);
        spec->Delay = TDuration::Zero();
        stores[percentile] = New<TInflightTracker>(NProfiling::TProfiler{}, spec);
    }

    auto createMessage = [&] (const std::string& id, ui64 systemTimestamp, ui64 eventTimestamp) {
        return MakeOutputMessage(TStreamId("stream"), schema, id, systemTimestamp, eventTimestamp);
    };

    for (ui64 i = 1747052000ull; i < 1747052002ull; ++i) {
        auto message = createMessage(ToString(i), i, i);
        for (const auto& [_, store] : stores) {
            store->Register(message);
        }
    }

    for (const auto& [_, store] : stores) {
        EXPECT_EQ(store->GetCount(), 2);
        EXPECT_EQ(store->GetMinSystemTimestamp()->Underlying(), 1747052000ull);
    }

    EXPECT_EQ(stores.at(100)->GetMinEventTimestamp()->Underlying(), 1747052000ull);
    EXPECT_EQ(stores.at(80)->GetMinEventTimestamp()->Underlying(), 1747052000ull);
    EXPECT_EQ(stores.at(60)->GetMinEventTimestamp()->Underlying(), 1747052000ull);
    EXPECT_EQ(stores.at(40)->GetMinEventTimestamp()->Underlying(), 1747052001ull);
    EXPECT_EQ(stores.at(20)->GetMinEventTimestamp()->Underlying(), 1747052001ull);
    EXPECT_EQ(stores.at(0)->GetMinEventTimestamp(), std::nullopt);

    for (ui64 i = 1747052002ull; i < 1747052005ull; ++i) {
        auto message = createMessage(ToString(i), i, i);
        for (const auto& [_, store] : stores) {
            store->Register(message);
        }
    }

    for (const auto& [_, store] : stores) {
        EXPECT_EQ(store->GetCount(), 5);
        EXPECT_EQ(store->GetMinSystemTimestamp()->Underlying(), 1747052000ull);
    }

    EXPECT_EQ(stores.at(100)->GetMinEventTimestamp()->Underlying(), 1747052000ull);
    EXPECT_EQ(stores.at(80)->GetMinEventTimestamp()->Underlying(), 1747052001ull);
    EXPECT_EQ(stores.at(60)->GetMinEventTimestamp()->Underlying(), 1747052002ull);
    EXPECT_EQ(stores.at(40)->GetMinEventTimestamp()->Underlying(), 1747052003ull);
    EXPECT_EQ(stores.at(20)->GetMinEventTimestamp()->Underlying(), 1747052004ull);
    EXPECT_EQ(stores.at(0)->GetMinEventTimestamp(), std::nullopt);


    for (ui64 i = 1747052005ull; i < 1747053000ull; ++i) {
        auto message = createMessage(ToString(i), i, i);
        for (const auto& [_, store] : stores) {
            store->Register(message);
        }
    }

    for (const auto& [_, store] : stores) {
        EXPECT_EQ(store->GetCount(), 1000);
        EXPECT_EQ(store->GetMinSystemTimestamp()->Underlying(), 1747052000ull);
    }

    EXPECT_EQ(stores.at(100)->GetMinEventTimestamp()->Underlying(), 1747052000ull);
    EXPECT_EQ(stores.at(80)->GetMinEventTimestamp()->Underlying(), 1747052200ull);
    EXPECT_EQ(stores.at(60)->GetMinEventTimestamp()->Underlying(), 1747052400ull);
    EXPECT_EQ(stores.at(40)->GetMinEventTimestamp()->Underlying(), 1747052600ull);
    EXPECT_EQ(stores.at(20)->GetMinEventTimestamp()->Underlying(), 1747052800ull);
    EXPECT_EQ(stores.at(0)->GetMinEventTimestamp(), std::nullopt);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TTestInflightStoreTest, Delay)
{
    const auto schema = New<NTableClient::TTableSchema>(
        std::vector{
            NTableClient::TColumnSchema("payload", NTableClient::EValueType::String),
        });

    auto spec = New<TWatermarkPercentileSpec>();
    spec->Value = TWatermarkPercentile(80);
    spec->Delay = TDuration::Seconds(60);
    const auto store = New<TInflightTracker>(NProfiling::TProfiler{}, spec);

    auto createMessage = [&] (const std::string& id, ui64 systemTimestamp, ui64 eventTimestamp) {
        return MakeOutputMessage(TStreamId("stream"), schema, id, systemTimestamp, eventTimestamp);
    };

    for (ui64 i = 1747052000ull; i < 1747052100ull; ++i) {
        auto message = createMessage(ToString(i), i, i);
        store->Register(message);
    }
    EXPECT_EQ(store->GetCount(), 100);
    EXPECT_EQ(store->GetMinSystemTimestamp()->Underlying(), 1747052000ull);
    EXPECT_EQ(store->GetMinEventTimestamp()->Underlying(), 1747052000ull);

    for (ui64 i = 1747052100ull; i < 1747053000ull; ++i) {
        auto message = createMessage(ToString(i), i, i);
        store->Register(message);
    }
    EXPECT_EQ(store->GetCount(), 1000);
    EXPECT_EQ(store->GetMinSystemTimestamp()->Underlying(), 1747052000ull);
    EXPECT_EQ(store->GetMinEventTimestamp()->Underlying(), 1747052140ull);
}

////////////////////////////////////////////////////////////////////////////////

// Naive reference implementation for stress testing.
class TReferenceInflightStore
{
public:
    explicit TReferenceInflightStore(double percentile, TDuration delay)
        : Percentile_(percentile)
        , Delay_(delay)
    { }

    void Register(const std::string& id, ui64 systemTs, ui64 eventTs)
    {
        YT_VERIFY(Messages_.emplace(id, std::make_pair(systemTs, eventTs)).second);
    }

    void Unregister(const std::string& id)
    {
        YT_VERIFY(Messages_.erase(id) == 1);
    }

    bool Contains(const std::string& id) const
    {
        return Messages_.contains(id);
    }

    std::optional<ui64> GetMinSystemTimestamp() const
    {
        if (Messages_.empty()) {
            return std::nullopt;
        }
        ui64 minTs = std::numeric_limits<ui64>::max();
        for (const auto& [_, v] : Messages_) {
            minTs = std::min(minTs, v.first);
        }
        return minTs;
    }

    std::optional<ui64> GetMinEventTimestamp() const
    {
        if (Messages_.empty()) {
            return std::nullopt;
        }
        int n = std::ssize(Messages_);
        // index = floor(N * (100 - percentile) / 100)
        int index = n * (100.0 - Percentile_) / 100.0;
        if (index >= n) {
            return std::nullopt;
        }
        // Collect and sort event timestamps.
        std::vector<ui64> eventTs;
        eventTs.reserve(n);
        for (const auto& [_, v] : Messages_) {
            eventTs.push_back(v.second);
        }
        std::sort(eventTs.begin(), eventTs.end());
        // Percentile element is at position index.
        ui64 rawPercentileTs = eventTs[index];
        ui64 delaySeconds = Delay_.Seconds();
        ui64 percentileTs = std::max(rawPercentileTs, delaySeconds) - delaySeconds;
        ui64 minEventTs = eventTs[0];
        return std::max(minEventTs, percentileTs);
    }

    int GetCount() const
    {
        return std::ssize(Messages_);
    }

private:
    double Percentile_;
    TDuration Delay_;
    THashMap<std::string, std::pair<ui64, ui64>> Messages_;
};

////////////////////////////////////////////////////////////////////////////////

TEST(TTestInflightStoreTest, Timer)
{
    // Timers should behave identically to messages for inflight tracking purposes.
    auto spec = New<TWatermarkPercentileSpec>();
    spec->Value = PreciseWatermarkPercentile;
    const auto store = New<TInflightTracker>(NProfiling::TProfiler{}, spec);

    auto createTimer = [&] (const std::string& id, ui64 systemTimestamp, ui64 eventTimestamp) {
        return MakeInputTimer("stream", id, systemTimestamp, eventTimestamp);
    };

    EXPECT_EQ(store->GetCount(), 0);
    EXPECT_EQ(store->GetMinSystemTimestamp(), std::nullopt);
    EXPECT_EQ(store->GetMinEventTimestamp(), std::nullopt);

    EXPECT_EQ(store->TryRegister(createTimer("1", 1747052600ull, 1747052500ull)), true);
    EXPECT_EQ(store->GetCount(), 1);
    EXPECT_EQ(store->GetMinSystemTimestamp()->Underlying(), 1747052600ull);
    EXPECT_EQ(store->GetMinEventTimestamp()->Underlying(), 1747052500ull);

    // Duplicate registration should be rejected.
    EXPECT_EQ(store->TryRegister(createTimer("1", 1747052400ull, 1747052400ull)), false);
    EXPECT_EQ(store->GetCount(), 1);
    EXPECT_EQ(store->GetMinSystemTimestamp()->Underlying(), 1747052600ull);

    EXPECT_EQ(store->TryRegister(createTimer("2", 1747052300ull, 1747052700ull)), true);
    EXPECT_EQ(store->GetCount(), 2);
    EXPECT_EQ(store->GetMinSystemTimestamp()->Underlying(), 1747052300ull);
    EXPECT_EQ(store->GetMinEventTimestamp()->Underlying(), 1747052500ull);

    EXPECT_EQ(store->TryUnregister(TMessageId("1")), true);
    EXPECT_EQ(store->GetCount(), 1);
    EXPECT_EQ(store->GetMinSystemTimestamp()->Underlying(), 1747052300ull);
    EXPECT_EQ(store->GetMinEventTimestamp()->Underlying(), 1747052700ull);

    EXPECT_EQ(store->TryUnregister(TMessageId("2")), true);
    EXPECT_EQ(store->GetCount(), 0);
    EXPECT_EQ(store->GetMinSystemTimestamp(), std::nullopt);
    EXPECT_EQ(store->GetMinEventTimestamp(), std::nullopt);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TTestInflightStoreTest, PercentileWithUnregister)
{
    // Verify that percentile is correctly maintained after unregistrations.
    // This exercises the two-heap rebalancing on removal.
    const auto schema = New<NTableClient::TTableSchema>(
        std::vector{
            NTableClient::TColumnSchema("payload", NTableClient::EValueType::String),
        });

    auto spec = New<TWatermarkPercentileSpec>();
    spec->Value = TWatermarkPercentile(80);
    spec->Delay = TDuration::Zero();
    const auto store = New<TInflightTracker>(NProfiling::TProfiler{}, spec);

    auto createMessage = [&] (const std::string& id, ui64 systemTimestamp, ui64 eventTimestamp) {
        return MakeOutputMessage(TStreamId("stream"), schema, id, systemTimestamp, eventTimestamp);
    };

    // Register 5 messages with event timestamps 1..5.
    // With percentile=80, LowerSize = floor(5 * 20 / 100) = 1.
    // Percentile element = 2nd smallest = event ts 2.
    // Result = max(min=1, percentile=2) = 2.
    for (ui64 i = 1; i <= 5; ++i) {
        store->Register(createMessage(ToString(i), i, i));
    }
    EXPECT_EQ(store->GetCount(), 5);
    EXPECT_EQ(store->GetMinEventTimestamp()->Underlying(), 2ull);

    // Remove the minimum (event ts 1). Now 4 elements: 2,3,4,5.
    // LowerSize = floor(4 * 20 / 100) = 0. Percentile = UpperHeap top = 2.
    // Result = max(min=2, percentile=2) = 2.
    store->Unregister(TMessageId("1"));
    EXPECT_EQ(store->GetCount(), 4);
    EXPECT_EQ(store->GetMinEventTimestamp()->Underlying(), 2ull);

    // Remove event ts 2. Now 3 elements: 3,4,5.
    // LowerSize = floor(3 * 20 / 100) = 0. Percentile = 3.
    // Result = max(min=3, percentile=3) = 3.
    store->Unregister(TMessageId("2"));
    EXPECT_EQ(store->GetCount(), 3);
    EXPECT_EQ(store->GetMinEventTimestamp()->Underlying(), 3ull);

    // Remove event ts 5 (the maximum). Now 2 elements: 3,4.
    // LowerSize = floor(2 * 20 / 100) = 0. Percentile = 3.
    // Result = max(min=3, percentile=3) = 3.
    store->Unregister(TMessageId("5"));
    EXPECT_EQ(store->GetCount(), 2);
    EXPECT_EQ(store->GetMinEventTimestamp()->Underlying(), 3ull);

    // Remove event ts 3. Now 1 element: 4.
    // LowerSize = floor(1 * 20 / 100) = 0. Percentile = 4.
    // Result = max(min=4, percentile=4) = 4.
    store->Unregister(TMessageId("3"));
    EXPECT_EQ(store->GetCount(), 1);
    EXPECT_EQ(store->GetMinEventTimestamp()->Underlying(), 4ull);

    store->Unregister(TMessageId("4"));
    EXPECT_EQ(store->GetCount(), 0);
    EXPECT_EQ(store->GetMinEventTimestamp(), std::nullopt);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TTestInflightStoreTest, IgnoreInflight)
{
    // Percentile == 0 means ignore inflight entirely: GetMinEventTimestamp always returns nullopt.
    const auto schema = New<NTableClient::TTableSchema>(
        std::vector{
            NTableClient::TColumnSchema("payload", NTableClient::EValueType::String),
        });

    auto spec = New<TWatermarkPercentileSpec>();
    spec->Value = IgnoreInflightWatermarkPercentile;
    spec->Delay = TDuration::Zero();
    const auto store = New<TInflightTracker>(NProfiling::TProfiler{}, spec);

    auto createMessage = [&] (const std::string& id, ui64 ts) {
        return MakeOutputMessage(TStreamId("stream"), schema, id, ts, ts);
    };

    EXPECT_EQ(store->GetMinEventTimestamp(), std::nullopt);

    store->Register(createMessage("1", 1747052000ull));
    EXPECT_EQ(store->GetCount(), 1);
    EXPECT_EQ(store->GetMinSystemTimestamp()->Underlying(), 1747052000ull);
    EXPECT_EQ(store->GetMinEventTimestamp(), std::nullopt);

    store->Register(createMessage("2", 1747052001ull));
    EXPECT_EQ(store->GetCount(), 2);
    EXPECT_EQ(store->GetMinEventTimestamp(), std::nullopt);

    store->Unregister(TMessageId("1"));
    EXPECT_EQ(store->GetMinEventTimestamp(), std::nullopt);

    store->Unregister(TMessageId("2"));
    EXPECT_EQ(store->GetMinEventTimestamp(), std::nullopt);
}

////////////////////////////////////////////////////////////////////////////////

// Stress test: compare TInflightTracker against TReferenceInflightStore
// on random sequences of register/unregister operations.
TEST(TTestInflightStoreTest, StressVsReference)
{
    const auto schema = New<NTableClient::TTableSchema>(
        std::vector{
            NTableClient::TColumnSchema("payload", NTableClient::EValueType::String),
        });

    // Test multiple (percentile, delay) combinations.
    struct TTestCase
    {
        double Percentile;
        TDuration Delay;
    };

    const std::vector<TTestCase> testCases = {
        {100.0, TDuration::Zero()},
        {80.0, TDuration::Zero()},
        {60.0, TDuration::Zero()},
        {50.0, TDuration::Zero()},
        {40.0, TDuration::Zero()},
        {20.0, TDuration::Zero()},
        {0.0, TDuration::Zero()},
        {80.0, TDuration::Seconds(10)},
        {60.0, TDuration::Seconds(50)},
        {50.0, TDuration::Seconds(25)},
        {100.0, TDuration::Seconds(100)},
    };

    for (const auto& tc : testCases) {
        auto spec = New<TWatermarkPercentileSpec>();
        spec->Value = TWatermarkPercentile(tc.Percentile);
        spec->Delay = tc.Delay;
        const auto store = New<TInflightTracker>(NProfiling::TProfiler{}, spec);
        TReferenceInflightStore ref(tc.Percentile, tc.Delay);

        auto createMessage = [&] (const std::string& id, ui64 systemTs, ui64 eventTs) {
            return MakeOutputMessage(TStreamId("stream"), schema, id, systemTs, eventTs);
        };

        auto checkEqual = [&] (int step) {
            SCOPED_TRACE(Format("percentile=%v delay=%v step=%v", tc.Percentile, tc.Delay, step));
            EXPECT_EQ(store->GetCount(), ref.GetCount());

            auto storeMinSys = store->GetMinSystemTimestamp();
            auto refMinSys = ref.GetMinSystemTimestamp();
            EXPECT_EQ(storeMinSys.has_value(), refMinSys.has_value());
            if (storeMinSys && refMinSys) {
                EXPECT_EQ(storeMinSys->Underlying(), *refMinSys);
            }

            auto storeMinEvent = store->GetMinEventTimestamp();
            auto refMinEvent = ref.GetMinEventTimestamp();
            EXPECT_EQ(storeMinEvent.has_value(), refMinEvent.has_value());
            if (storeMinEvent && refMinEvent) {
                EXPECT_EQ(storeMinEvent->Underlying(), *refMinEvent);
            }
        };

        // Use a fixed seed for reproducibility.
        std::mt19937 rng(42 + static_cast<int>(tc.Percentile * 100) + static_cast<int>(tc.Delay.Seconds()));

        // Pool of possible IDs.
        const int IdPoolSize = 20;
        // Pool of possible timestamps (small range to get duplicates).
        const ui64 TsRange = 30;
        const ui64 TsBase = 1747052000ull;

        std::vector<std::string> registered;

        int step = 0;
        for (int iter = 0; iter < 3000; ++iter, ++step) {
            // Decide: register new or unregister existing.
            bool doRegister = registered.empty() || (std::uniform_int_distribution<int>(0, 2)(rng) != 0);

            if (doRegister && std::ssize(registered) < IdPoolSize) {
                // Pick a random ID not yet registered.
                std::string id = ToString(std::uniform_int_distribution<int>(0, IdPoolSize * 3)(rng));
                if (!ref.Contains(id)) {
                    ui64 sysTs = TsBase + std::uniform_int_distribution<ui64>(0, TsRange)(rng);
                    ui64 evTs = TsBase + std::uniform_int_distribution<ui64>(0, TsRange)(rng);
                    store->Register(createMessage(id, sysTs, evTs));
                    ref.Register(id, sysTs, evTs);
                    registered.push_back(id);
                    checkEqual(step);
                }
            } else if (!registered.empty()) {
                // Unregister a random existing element.
                int idx = std::uniform_int_distribution<int>(0, std::ssize(registered) - 1)(rng);
                std::string id = registered[idx];
                registered.erase(registered.begin() + idx);
                store->Unregister(TMessageId(id));
                ref.Unregister(id);
                checkEqual(step);
            }
        }

        // Drain all remaining.
        while (!registered.empty()) {
            std::string id = registered.back();
            registered.pop_back();
            store->Unregister(TMessageId(id));
            ref.Unregister(id);
            checkEqual(step++);
        }

        EXPECT_EQ(store->GetCount(), 0);
        EXPECT_EQ(store->GetMinSystemTimestamp(), std::nullopt);
        EXPECT_EQ(store->GetMinEventTimestamp(), std::nullopt);
    }

    // Additional stress: insert elements in strictly decreasing event-timestamp order
    // with a query between each insert. This specifically exercises the scenario where
    // UpperEventHeap_ becomes empty after a rebalance and the next insert has a smaller
    // timestamp than the current LowerEventHeap_ max.
    for (double percentile : {50.0, 60.0, 80.0}) {
        auto spec = New<TWatermarkPercentileSpec>();
        spec->Value = TWatermarkPercentile(percentile);
        spec->Delay = TDuration::Zero();
        const auto store = New<TInflightTracker>(NProfiling::TProfiler{}, spec);
        TReferenceInflightStore ref(percentile, TDuration::Zero());

        auto createMessage = [&] (const std::string& id, ui64 eventTs) {
            return MakeOutputMessage(TStreamId("stream"), schema, id, /*systemTs*/ 1000, eventTs);
        };

        auto checkEqual = [&] (int step) {
            SCOPED_TRACE(Format("decreasing percentile=%v step=%v", percentile, step));
            auto storeMinEvent = store->GetMinEventTimestamp();
            auto refMinEvent = ref.GetMinEventTimestamp();
            EXPECT_EQ(storeMinEvent.has_value(), refMinEvent.has_value());
            if (storeMinEvent && refMinEvent) {
                EXPECT_EQ(storeMinEvent->Underlying(), *refMinEvent);
            }
        };

        // Insert 20 elements with decreasing event timestamps (100, 99, ..., 81),
        // querying after each insert to trigger rebalance.
        for (int i = 0; i < 20; ++i) {
            ui64 evTs = 100 - i;
            std::string id = ToString(i);
            store->Register(createMessage(id, evTs));
            ref.Register(id, 1000, evTs);
            checkEqual(i);
        }

        // Unregister in reverse order (smallest timestamps first).
        for (int i = 19; i >= 0; --i) {
            std::string id = ToString(i);
            store->Unregister(TMessageId(id));
            ref.Unregister(id);
            checkEqual(20 + (19 - i));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

// Regression test for the percentile=100 fast-path that skips the percentile heaps.
// The optimization saves 2×N×8 B of vector storage at peak inflight; this test verifies
// that the skipped-percentile-heaps path produces identical observable behavior for the
// percentile==100 default — covering insert / GetMin / unregister cycles, including
// out-of-order unregister.
TEST(TTestInflightStoreTest, PercentilePreciseSkipsHeaps)
{
    const auto schema = New<NTableClient::TTableSchema>(
        std::vector{
            NTableClient::TColumnSchema("payload", NTableClient::EValueType::String),
        });

    auto spec = New<TWatermarkPercentileSpec>();
    spec->Value = PreciseWatermarkPercentile;
    spec->Delay = TDuration::Zero();
    const auto store = New<TInflightTracker>(NProfiling::TProfiler{}, spec);

    auto createMessage = [&] (const std::string& id, ui64 systemTimestamp, ui64 eventTimestamp) {
        return MakeOutputMessage(TStreamId("stream"), schema, id, systemTimestamp, eventTimestamp);
    };

    // Register messages with arbitrary order of event timestamps.
    const std::vector<std::pair<std::string, ui64>> messages = {
        {"m3", 1747052300},
        {"m1", 1747052100},
        {"m5", 1747052500},
        {"m2", 1747052200},
        {"m4", 1747052400},
    };
    for (const auto& [id, ts] : messages) {
        store->Register(createMessage(id, /*systemTs*/ ts, /*eventTs*/ ts));
    }
    EXPECT_EQ(store->GetCount(), 5);

    // Percentile=100 → min event timestamp == min over all entries.
    EXPECT_EQ(store->GetMinEventTimestamp()->Underlying(), 1747052100ull);
    EXPECT_EQ(store->GetMinSystemTimestamp()->Underlying(), 1747052100ull);

    // Unregister in non-monotonic order — the percentile fast-path must still maintain
    // MinEventTimestampHeap correctly.
    store->Unregister(TMessageId("m3"));
    EXPECT_EQ(store->GetMinEventTimestamp()->Underlying(), 1747052100ull);

    store->Unregister(TMessageId("m1"));
    EXPECT_EQ(store->GetMinEventTimestamp()->Underlying(), 1747052200ull);

    store->Unregister(TMessageId("m5"));
    EXPECT_EQ(store->GetMinEventTimestamp()->Underlying(), 1747052200ull);

    store->Unregister(TMessageId("m4"));
    EXPECT_EQ(store->GetMinEventTimestamp()->Underlying(), 1747052200ull);

    store->Unregister(TMessageId("m2"));
    EXPECT_EQ(store->GetCount(), 0);
    EXPECT_EQ(store->GetMinEventTimestamp(), std::nullopt);
    EXPECT_EQ(store->GetMinSystemTimestamp(), std::nullopt);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TTestMultiInflightStoreTest, TwoStreams)
{
    const auto schema = New<NTableClient::TTableSchema>(
        std::vector{
            NTableClient::TColumnSchema("payload", NTableClient::EValueType::String),
        });
    auto spec = New<TWatermarkPercentileSpec>();
    spec->Value = PreciseWatermarkPercentile;
    const auto store = New<TMultiInflightTracker>(
        NProfiling::TProfiler{},
        std::vector{TStreamId("a"), TStreamId("b")},
        spec);

    auto createMessage = [&] (const std::string& streamId, const std::string& id, ui64 systemTimestamp, ui64 eventTimestamp) {
        return MakeOutputMessage(TStreamId(streamId), schema, id, systemTimestamp, eventTimestamp);
    };

    auto messageMeta = [] (const std::string& streamId, const std::string& id) {
        TMessageMeta meta;
        meta.StreamId = TStreamId(streamId);
        meta.MessageId = TMessageId(id);
        return meta;
    };

    // Same-stream burst: the SmallTrackers_ linear scan is taken on every
    // resolve since the stream count (2) is <= MaxSmallTrackerCount.
    EXPECT_EQ(store->TryRegister(createMessage("a", "a1", 100, 50)), true);
    EXPECT_EQ(store->TryRegister(createMessage("a", "a2", 110, 60)), true);
    EXPECT_EQ(store->TryRegister(createMessage("a", "a1", 1, 1)), false);
    EXPECT_EQ(store->GetTotalCount(), 2);
    EXPECT_TRUE(store->Contains(messageMeta("a", "a1")));
    EXPECT_FALSE(store->Contains(messageMeta("a", "a3")));

    // Alternating streams: each resolve still walks SmallTrackers_ linearly.
    EXPECT_EQ(store->TryRegister(createMessage("b", "b1", 200, 150)), true);
    EXPECT_EQ(store->TryRegister(createMessage("a", "a3", 120, 70)), true);
    EXPECT_EQ(store->TryRegister(createMessage("b", "b2", 210, 160)), true);
    EXPECT_EQ(store->GetTotalCount(), 5);
    EXPECT_TRUE(store->Contains(messageMeta("b", "b1")));
    EXPECT_TRUE(store->Contains(messageMeta("a", "a3")));

    // Same per-stream counts and byte sizes regardless of call order.
    auto sizes = store->GetCountAndByteSizes();
    EXPECT_EQ(sizes.at(TStreamId("a")).first, 3);
    EXPECT_EQ(sizes.at(TStreamId("b")).first, 2);

    // Cross-stream unregister with one missing id; SyncCounters flushes deltas
    // accumulated across both register and unregister bursts.
    EXPECT_EQ(store->TryUnregister(messageMeta("a", "a1")), true);
    EXPECT_EQ(store->TryUnregister(messageMeta("a", "missing")), false);
    EXPECT_EQ(store->TryUnregister(messageMeta("b", "b1")), true);
    store->SyncCounters();
    EXPECT_EQ(store->GetTotalCount(), 3);
    EXPECT_FALSE(store->Contains(messageMeta("a", "a1")));
    EXPECT_FALSE(store->Contains(messageMeta("b", "b1")));
}

////////////////////////////////////////////////////////////////////////////////

// Exercises the hashmap fallback path of TMultiInflightTracker. With three
// streams the constructor leaves SmallTrackers_ empty and every ResolveTracker
// call goes through InflightTrackers_.at(streamId).
TEST(TTestMultiInflightStoreTest, ThreeStreams)
{
    const auto schema = New<NTableClient::TTableSchema>(
        std::vector{
            NTableClient::TColumnSchema("payload", NTableClient::EValueType::String),
        });
    auto spec = New<TWatermarkPercentileSpec>();
    spec->Value = PreciseWatermarkPercentile;
    const auto store = New<TMultiInflightTracker>(
        NProfiling::TProfiler{},
        std::vector{TStreamId("a"), TStreamId("b"), TStreamId("c")},
        spec);

    auto createMessage = [&] (const std::string& streamId, const std::string& id, ui64 systemTimestamp, ui64 eventTimestamp) {
        return MakeOutputMessage(TStreamId(streamId), schema, id, systemTimestamp, eventTimestamp);
    };

    auto messageMeta = [] (const std::string& streamId, const std::string& id) {
        TMessageMeta meta;
        meta.StreamId = TStreamId(streamId);
        meta.MessageId = TMessageId(id);
        return meta;
    };

    EXPECT_EQ(store->TryRegister(createMessage("a", "a1", 100, 50)), true);
    EXPECT_EQ(store->TryRegister(createMessage("b", "b1", 110, 60)), true);
    EXPECT_EQ(store->TryRegister(createMessage("c", "c1", 120, 70)), true);
    EXPECT_EQ(store->TryRegister(createMessage("a", "a2", 130, 80)), true);
    EXPECT_EQ(store->TryRegister(createMessage("c", "c1", 1, 1)), false);
    store->SyncCounters();

    EXPECT_EQ(store->GetTotalCount(), 4);
    auto sizes = store->GetCountAndByteSizes();
    EXPECT_EQ(sizes.at(TStreamId("a")).first, 2);
    EXPECT_EQ(sizes.at(TStreamId("b")).first, 1);
    EXPECT_EQ(sizes.at(TStreamId("c")).first, 1);
    EXPECT_TRUE(store->Contains(messageMeta("a", "a1")));
    EXPECT_TRUE(store->Contains(messageMeta("c", "c1")));
    EXPECT_FALSE(store->Contains(messageMeta("a", "missing")));

    EXPECT_EQ(store->TryUnregister(messageMeta("b", "b1")), true);
    EXPECT_EQ(store->TryUnregister(messageMeta("c", "missing")), false);
    EXPECT_EQ(store->TryUnregister(messageMeta("a", "a1")), true);
    store->SyncCounters();

    EXPECT_EQ(store->GetTotalCount(), 2);
    auto sizesAfter = store->GetCountAndByteSizes();
    EXPECT_EQ(sizesAfter.at(TStreamId("a")).first, 1);
    EXPECT_EQ(sizesAfter.at(TStreamId("b")).first, 0);
    EXPECT_EQ(sizesAfter.at(TStreamId("c")).first, 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
