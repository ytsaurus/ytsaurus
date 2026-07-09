#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/public.h>

#include <yt/yt/flow/library/cpp/tables/context.h>
#include <yt/yt/flow/library/cpp/tables/input_messages.h>

#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/cache/rpc.h>

namespace NYT::NFlow {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NLogging;
using namespace NObjectClient;
using namespace NYPath;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Test");

////////////////////////////////////////////////////////////////////////////////

class TTestInputMessages
    : public ::testing::Test
{
protected:
    NApi::IClientPtr Client_;

    void SetUp() override
    {
        // Connect through the RPC proxy using YT_PROXY (exported by the Flow recipe). Going
        // through the proxy means the pipeline node type handler that bootstraps a pipeline's
        // child tables runs from the package binaries, not from the (possibly older) code linked
        // into this test binary.
        Client_ = NClient::NCache::CreateClient();

        WaitFor(Client_->CreateNode(GetPipelinePath(), EObjectType::Pipeline))
            .ThrowOnError();
        WaitUntilTableMounted(YPathJoin(GetPipelinePath(), "input_messages"));
        WaitUntilTableMounted(YPathJoin(GetPipelinePath(), "compact_input_messages"));
    }

    void TearDown() override
    {
        auto tryRemove = [&] {
            return WaitFor(Client_->RemoveNode(GetPipelinePath())).IsOK();
        };
        WaitForPredicate(tryRemove, /*iterationCount*/ 100, /*period*/ TDuration::MilliSeconds(200));
    }

    static TYPath GetPipelinePath()
    {
        return "//pipeline_input_messages";
    }

    void WaitUntilTableMounted(const TYPath& tablePath)
    {
        WaitForPredicate(
            [&] {
                auto state = WaitFor(Client_->GetNode(tablePath + "/@tablet_state")).ValueOrThrow();
                return NYTree::ConvertTo<std::string>(state) == "mounted";
            },
            /*iterationCount*/ 100,
            /*period*/ TDuration::MilliSeconds(500));
    }

    NTables::TContextPtr PrepareTablesContext()
    {
        auto context = New<NTables::TContext>();
        context->Client = Client_;
        context->PipelinePath = GetPipelinePath();
        context->LoadThroughputThrottler = New<TLoadThroughputThrottler>(
            CreateNamedUnlimitedThroughputThrottler("test", NProfiling::TProfiler()),
            Logger(),
            NProfiling::TProfiler());
        context->Logger = Logger();
        context->Profiler = NProfiling::TProfiler();
        return context;
    }

    // Creates a minimal TInputMessages::TMessage with the given ids.
    NTables::TInputMessages::TMessage MakeTableMessage(
        const std::string& messageId,
        ui64 systemTimestamp = 100)
    {
        return NTables::TInputMessages::TMessage{
            .Key = MakeKey(ToString(messageId)),
            .MessageId = TMessageId(messageId),
            .SystemTimestamp = TSystemTimestamp(systemTimestamp),
        };
    }

    // Creates a TInputMessages::TMessage with a single uint64 key column, as required by the
    // compact table (whose deduplication key reads key[0] as a uint64 hash).
    NTables::TInputMessages::TMessage MakeUintTableMessage(
        ui64 keyHash,
        const std::string& messageId,
        ui64 systemTimestamp = 100)
    {
        return NTables::TInputMessages::TMessage{
            .Key = MakeKey(keyHash),
            .MessageId = TMessageId(messageId),
            .SystemTimestamp = TSystemTimestamp(systemTimestamp),
        };
    }

    // Commits a Write() call inside a dynamic table transaction.
    void WriteMessages(
        const NTables::IInputMessagesPtr& table,
        const TComputationId& computationId,
        const std::vector<NTables::TInputMessages::TMessage>& messages)
    {
        auto tx = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();
        table->Write(tx, computationId, messages);
        WaitFor(tx->Commit()).ThrowOnError();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_W(TTestInputMessages, WriteAndContains)
{
    auto tablesContext = PrepareTablesContext();
    auto table = New<NTables::TInputMessages>(tablesContext);

    const TComputationId computationId("comp-1");
    auto msg = MakeTableMessage("msg-1");

    // Before write — not present.
    auto before = WaitFor(table->Contains(computationId, {msg})).ValueOrThrow();
    ASSERT_EQ(std::ssize(before), 1);
    EXPECT_FALSE(before[0]);

    WriteMessages(table, computationId, {msg});

    // After write — present.
    auto after = WaitFor(table->Contains(computationId, {msg})).ValueOrThrow();
    ASSERT_EQ(std::ssize(after), 1);
    EXPECT_TRUE(after[0]);
}

TEST_W(TTestInputMessages, ContainsEmpty)
{
    auto tablesContext = PrepareTablesContext();
    auto table = New<NTables::TInputMessages>(tablesContext);

    const TComputationId computationId("comp-empty");

    // Empty message list — result must also be empty.
    auto result = WaitFor(table->Contains(computationId, {})).ValueOrThrow();
    EXPECT_TRUE(result.empty());
}

TEST_W(TTestInputMessages, WriteMultipleComputations)
{
    auto tablesContext = PrepareTablesContext();
    auto table = New<NTables::TInputMessages>(tablesContext);

    const TComputationId comp1("comp-a");
    const TComputationId comp2("comp-b");
    auto msg = MakeTableMessage("shared-msg");

    WriteMessages(table, comp1, {msg});

    // Written under comp1 — present for comp1, absent for comp2.
    auto r1 = WaitFor(table->Contains(comp1, {msg})).ValueOrThrow();
    EXPECT_TRUE(r1[0]);

    auto r2 = WaitFor(table->Contains(comp2, {msg})).ValueOrThrow();
    EXPECT_FALSE(r2[0]);
}

TEST_W(TTestInputMessages, ContainsMissingRows)
{
    auto tablesContext = PrepareTablesContext();
    auto table = New<NTables::TInputMessages>(tablesContext);

    const TComputationId computationId("comp-missing");
    auto present = MakeTableMessage("present-msg");
    auto missing = MakeTableMessage("missing-msg");

    WriteMessages(table, computationId, {present});

    auto result = WaitFor(table->Contains(computationId, {present, missing})).ValueOrThrow();
    ASSERT_EQ(std::ssize(result), 2);
    EXPECT_TRUE(result[0]);
    EXPECT_FALSE(result[1]);
}

// Verifies that messages with the same MessageId but different Key are stored independently.
TEST_W(TTestInputMessages, SameMessageIdDifferentKeyAreIndependent)
{
    auto tablesContext = PrepareTablesContext();
    auto table = New<NTables::TInputMessages>(tablesContext);

    const TComputationId computationId("comp-key-isolation");

    // Two messages: same MessageId, different Key.
    NTables::TInputMessages::TMessage msgA{
        .Key = MakeKey(ToString("key-a")),
        .MessageId = TMessageId("shared-id"),
        .SystemTimestamp = TSystemTimestamp(100),
    };
    NTables::TInputMessages::TMessage msgB{
        .Key = MakeKey(ToString("key-b")),
        .MessageId = TMessageId("shared-id"),
        .SystemTimestamp = TSystemTimestamp(100),
    };

    // Write only msgA.
    WriteMessages(table, computationId, {msgA});

    // msgA is present; msgB (same MessageId, different Key) is absent.
    auto resultA = WaitFor(table->Contains(computationId, {msgA})).ValueOrThrow();
    ASSERT_EQ(std::ssize(resultA), 1);
    EXPECT_TRUE(resultA[0]);

    auto resultB = WaitFor(table->Contains(computationId, {msgB})).ValueOrThrow();
    ASSERT_EQ(std::ssize(resultB), 1);
    EXPECT_FALSE(resultB[0]);
}

////////////////////////////////////////////////////////////////////////////////

TEST_W(TTestInputMessages, CompactWriteAndContains)
{
    auto tablesContext = PrepareTablesContext();
    auto table = New<NTables::TCompactInputMessages>(tablesContext);

    const TComputationId computationId("comp-1");
    auto msg = MakeUintTableMessage(/*keyHash*/ 42, "msg-1");

    // Before write — not present.
    auto before = WaitFor(table->Contains(computationId, {msg})).ValueOrThrow();
    ASSERT_EQ(std::ssize(before), 1);
    EXPECT_FALSE(before[0]);

    WriteMessages(table, computationId, {msg});

    // After write — present.
    auto after = WaitFor(table->Contains(computationId, {msg})).ValueOrThrow();
    ASSERT_EQ(std::ssize(after), 1);
    EXPECT_TRUE(after[0]);
}

TEST_W(TTestInputMessages, CompactContainsEmpty)
{
    auto tablesContext = PrepareTablesContext();
    auto table = New<NTables::TCompactInputMessages>(tablesContext);

    const TComputationId computationId("comp-empty");

    auto result = WaitFor(table->Contains(computationId, {})).ValueOrThrow();
    EXPECT_TRUE(result.empty());
}

TEST_W(TTestInputMessages, CompactWriteMultipleComputations)
{
    auto tablesContext = PrepareTablesContext();
    auto table = New<NTables::TCompactInputMessages>(tablesContext);

    const TComputationId comp1("comp-a");
    const TComputationId comp2("comp-b");
    auto msg = MakeUintTableMessage(/*keyHash*/ 7, "shared-msg");

    WriteMessages(table, comp1, {msg});

    // Written under comp1 — present for comp1, absent for comp2.
    auto r1 = WaitFor(table->Contains(comp1, {msg})).ValueOrThrow();
    EXPECT_TRUE(r1[0]);

    auto r2 = WaitFor(table->Contains(comp2, {msg})).ValueOrThrow();
    EXPECT_FALSE(r2[0]);
}

TEST_W(TTestInputMessages, CompactContainsMissingRows)
{
    auto tablesContext = PrepareTablesContext();
    auto table = New<NTables::TCompactInputMessages>(tablesContext);

    const TComputationId computationId("comp-missing");
    auto present = MakeUintTableMessage(/*keyHash*/ 1, "present-msg");
    auto missing = MakeUintTableMessage(/*keyHash*/ 2, "missing-msg");

    WriteMessages(table, computationId, {present});

    auto result = WaitFor(table->Contains(computationId, {present, missing})).ValueOrThrow();
    ASSERT_EQ(std::ssize(result), 2);
    EXPECT_TRUE(result[0]);
    EXPECT_FALSE(result[1]);
}

// Verifies that for the compact table messages with the same MessageId but different key hash
// are stored independently.
TEST_W(TTestInputMessages, CompactSameMessageIdDifferentKeyAreIndependent)
{
    auto tablesContext = PrepareTablesContext();
    auto table = New<NTables::TCompactInputMessages>(tablesContext);

    const TComputationId computationId("comp-key-isolation");

    auto msgA = MakeUintTableMessage(/*keyHash*/ 100, "shared-id");
    auto msgB = MakeUintTableMessage(/*keyHash*/ 200, "shared-id");

    WriteMessages(table, computationId, {msgA});

    auto resultA = WaitFor(table->Contains(computationId, {msgA})).ValueOrThrow();
    ASSERT_EQ(std::ssize(resultA), 1);
    EXPECT_TRUE(resultA[0]);

    auto resultB = WaitFor(table->Contains(computationId, {msgB})).ValueOrThrow();
    ASSERT_EQ(std::ssize(resultB), 1);
    EXPECT_FALSE(resultB[0]);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
