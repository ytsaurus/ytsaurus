#include <yt/yt/flow/examples/cpp/url_downloader/lib/url_downloader_functions.h>

#include <yt/yt/flow/library/cpp/process_function/testing/entity_builders.h>
#include <yt/yt/flow/library/cpp/process_function/testing/process_function_test_harness.h>
#include <yt/yt/flow/library/cpp/process_function/testing/recording_output_collector.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_runtime_context.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_state_environment.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;
using namespace NYT::NFlow::NTesting;
using namespace NYT::NFlow::NExample;

////////////////////////////////////////////////////////////////////////////////

// The output message must be registered so the runtime context can validate/convert it.
YT_FLOW_DEFINE_YSON_MESSAGE(TProcessedUrlMessage);

////////////////////////////////////////////////////////////////////////////////

// Group-by key schema, matching the spec's group_by_schema (hash, host).
TTableSchemaPtr HostKeySchema()
{
    return ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        "[{name=hash;type=uint64;sort_order=ascending};"
        "{name=host;type=string;sort_order=ascending}]")));
}

TInputMessageConstPtr MakeUrl(const TKey& key, const std::string& host, const std::string& url)
{
    return MakeTestMessage("urls", key, GetYsonMessagePayloadSchema<TUrlMessage>(), [&] (TMessageBuilder& builder) {
        builder.Payload().Set(host, "host");
        builder.Payload().Set(url, "url");
    });
}

IRuntimeContextPtr MakeContext(TSystemTimestamp currentTimestamp)
{
    return TTestRuntimeContextBuilder()
        .SetKeySchema(HostKeySchema())
        .SetCurrentTimestamp(currentTimestamp)
        .RegisterStream<TProcessedUrlMessage>("processed_urls")
        .Build();
}

// Recomputes the deterministic trigger timestamp of GetNextHostCheck for the default
// CheckHostPeriod (5s), so a test can assert the scheduled timer exactly.
TSystemTimestamp ExpectedNextHostCheck(const std::string& host, ui64 currentTimestamp)
{
    ui64 period = 5;
    ui64 shift = THash<std::string>()(host) % period;
    ui64 next = currentTimestamp - currentTimestamp % period + shift + period;
    return TSystemTimestamp{next};
}

// Runs |body| on a serialized invoker, as the worker does. The async downloader asserts
// serialized-invoker affinity and schedules its executor via GetCurrentInvoker().
template <class TBody>
void RunSerialized(const TBody& body)
{
    auto queue = New<TActionQueue>("UrlDownloaderTest");
    WaitFor(BIND(body).AsyncVia(queue->GetInvoker()).Run())
        .ThrowOnError();
    queue->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TUrlDownloaderExampleTest, MessageSchedulesHostCheckTimer)
{
    TTestStateEnvironment stateEnv(HostKeySchema());
    ui64 currentTimestamp = 42;
    TProcessFunctionTestHarness harness(
        stateEnv,
        New<TLimitedUrlDownloadFunction>(),
        MakeContext(TSystemTimestamp(currentTimestamp)));

    auto key = MakeKey(ui64(1), std::string("example.com"));

    RunSerialized([&] {
        harness.RunEpoch({MakeUrl(key, "example.com", "example.com/page-12")});
    });

    // Downloading is asynchronous, so no processed URL is emitted within the epoch.
    EXPECT_TRUE(harness.GetMessages().empty());

    // The message must schedule exactly one host-check timer at the expected time.
    ASSERT_EQ(std::ssize(harness.GetTimers()), 1);
    EXPECT_EQ(harness.GetTimers()[0].TriggerTimestamp, ExpectedNextHostCheck("example.com", currentTimestamp));
}

TEST(TUrlDownloaderExampleTest, TimerOnEmptyHostClearsStateAndEmitsNothing)
{
    TTestStateEnvironment stateEnv(HostKeySchema());
    TProcessFunctionTestHarness harness(
        stateEnv,
        New<TLimitedUrlDownloadFunction>(),
        MakeContext(TSystemTimestamp(100)));

    auto key = MakeKey(ui64(2), std::string("empty.example"));

    // A timer for a key that never received a message: the host has no URLs, so nothing is
    // emitted, no follow-up timer is scheduled, and the (empty) state is cleared.
    RunSerialized([&] {
        harness.RunEpoch({}, {MakeTestTimer(key, TSystemTimestamp(200), HostKeySchema(), "timer")});
    });

    EXPECT_TRUE(harness.GetMessages().empty());
    EXPECT_TRUE(harness.GetTimers().empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
