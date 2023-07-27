#include <benchmark/benchmark.h>

#include <yt/yt/core/yson/consumer.h>
#include <yt/yt/core/yson/parser.h>
#include <yt/yt/core/yson/null_consumer.h>
#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/resource/resource.h>

#include <util/random/fast.h>

using namespace NYT::NYson;
using namespace NYT::NYTree;

////////////////////////////////////////////////////////////////////////////////

static TString GenerateYsonListInt64(int listSize, i64 maxInt)
{
    TFastRng64 rng(42);

    TString ysonData;
    {
        TStringOutput out(ysonData);
        TBufferedBinaryYsonWriter writer(&out);
        writer.OnBeginList();
        for (auto i = 0; i < listSize; ++i) {
            writer.OnListItem();
            writer.OnInt64Scalar(rng.Uniform(0, maxInt));
        }
        writer.OnEndList();
        writer.Flush();
    }
    return ysonData;
}

static void ListInt64_PullParser(benchmark::State& state)
{
    auto ysonData = GenerateYsonListInt64(100, 5000);
    for (auto _ : state) {
        TMemoryInput in(ysonData);
        TYsonPullParser parser(&in, EYsonType::Node);
        TYsonPullParserCursor cursor(&parser);
        YT_VERIFY(cursor->GetType() == EYsonItemType::BeginList);
        cursor.Next();
        ui64 sum = 0;
        for (; cursor->GetType() != EYsonItemType::EndList; cursor.Next()) {
            YT_VERIFY(cursor->GetType() == EYsonItemType::Int64Value);
            sum += cursor->UncheckedAsInt64();
        }
        cursor.Next();
        YT_VERIFY(cursor->GetType() == EYsonItemType::EndOfStream);
        Y_UNUSED(sum);
    }
}
BENCHMARK(ListInt64_PullParser);

static void ListInt64_ParseYsonStringBuffer(benchmark::State& state)
{
    auto ysonData = GenerateYsonListInt64(100, 5000);

    for (auto _ : state) {
        TMemoryInput in(ysonData);
        ParseYsonStringBuffer(ysonData, EYsonType::Node, GetNullYsonConsumer());
    }
}
BENCHMARK(ListInt64_ParseYsonStringBuffer);

static void ListInt64_PullParser_Transfer(benchmark::State& state)
{
    auto ysonData = GenerateYsonListInt64(100, 5000);

    for (auto _ : state) {
        TMemoryInput in(ysonData);
        TYsonPullParser parser(&in, EYsonType::Node);
        TYsonPullParserCursor cursor(&parser);
        YT_VERIFY(cursor->GetType() == EYsonItemType::BeginList);
        cursor.TransferComplexValue(GetNullYsonConsumer());
    }
}
BENCHMARK(ListInt64_PullParser_Transfer);

////////////////////////////////////////////////////////////////////////////////

static TString GetEventLogYson()
{
    auto eventLog = NResource::Find("event_log_small.yson");
    YT_VERIFY(!eventLog.empty() && eventLog.back() == ';');

    TString result;
    while (result.size() < 100_MB) {
        result += eventLog;
    }
    return result;
}


static void EventLog_PullParser(benchmark::State& state)
{
    auto ysonData = GetEventLogYson();
    for (auto _ : state) {
        TMemoryInput in(ysonData);
        TYsonPullParser parser(&in, EYsonType::ListFragment);
        TYsonPullParserCursor cursor(&parser);
        YT_VERIFY(cursor.TryConsumeFragmentStart());
        for (; cursor->GetType() != EYsonItemType::EndOfStream; cursor.Next()) {
        }
        Y_UNUSED(parser.GetTotalReadSize());
    }
}
BENCHMARK(EventLog_PullParser);

static void EventLog_ParseYsonStringBuffer(benchmark::State& state)
{
    auto ysonData = GetEventLogYson();

    for (auto _ : state) {
        TMemoryInput in(ysonData);
        ParseYsonStringBuffer(ysonData, EYsonType::ListFragment, GetNullYsonConsumer());
    }
}
BENCHMARK(EventLog_ParseYsonStringBuffer);
