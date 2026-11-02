#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/timer_page.h>

#include <yt/yt/flow/library/cpp/pipeline_tables/public.h>

#include <yt/yt/flow/library/cpp/tables/context.h>
#include <yt/yt/flow/library/cpp/tables/timer_pages.h>

#include <yt/yt/flow/library/cpp/native_client/public.h>

#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/cache/rpc.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/client/unittests/mock/client.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

namespace NYT::NFlow {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, Logger, "Test");

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCountingThrottler)

// Counts the throttle requests of the load throttler; every select of a page read issues exactly one.
class TCountingThrottler
    : public IReconfigurableThroughputThrottler
{
public:
    explicit TCountingThrottler(IReconfigurableThroughputThrottlerPtr underlying)
        : Underlying_(std::move(underlying))
    { }

    i64 GetThrottleCount() const
    {
        return ThrottleCount_.load();
    }

    TFuture<void> Throttle(i64 amount) override
    {
        ++ThrottleCount_;
        return Underlying_->Throttle(amount);
    }

    bool TryAcquire(i64 amount) override
    {
        return Underlying_->TryAcquire(amount);
    }

    i64 TryAcquireAvailable(i64 amount) override
    {
        return Underlying_->TryAcquireAvailable(amount);
    }

    void Acquire(i64 amount) override
    {
        Underlying_->Acquire(amount);
    }

    void Release(i64 amount) override
    {
        Underlying_->Release(amount);
    }

    bool IsOverdraft() override
    {
        return Underlying_->IsOverdraft();
    }

    i64 GetQueueTotalAmount() const override
    {
        return Underlying_->GetQueueTotalAmount();
    }

    TDuration GetEstimatedOverdraftDuration() const override
    {
        return Underlying_->GetEstimatedOverdraftDuration();
    }

    i64 GetAvailable() const override
    {
        return Underlying_->GetAvailable();
    }

    void Reconfigure(TThroughputThrottlerConfigPtr config) override
    {
        Underlying_->Reconfigure(std::move(config));
    }

    void SetLimit(std::optional<double> limit) override
    {
        Underlying_->SetLimit(limit);
    }

    std::optional<double> GetLimit() const override
    {
        return Underlying_->GetLimit();
    }

    TDuration GetPeriod() const override
    {
        return Underlying_->GetPeriod();
    }

    TFuture<void> GetAvailableFuture() override
    {
        return Underlying_->GetAvailableFuture();
    }

    TThroughputThrottlerConfigPtr GetConfig() const override
    {
        return Underlying_->GetConfig();
    }

private:
    const IReconfigurableThroughputThrottlerPtr Underlying_;
    std::atomic<i64> ThrottleCount_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TCountingThrottler)

////////////////////////////////////////////////////////////////////////////////

// The three tables are created straight from the catalogue definitions rather than through a
// pipeline node, so the test does not depend on the table set of the packaged YT binary.
class TTestTimerPages
    : public ::testing::Test
{
protected:
    NApi::IClientPtr Client_;
    NTables::TTimerPagesPtr Table_;

    void SetUp() override
    {
        Client_ = NClient::NCache::CreateClient();

        WaitFor(Client_->CreateNode(GetPipelinePath(), EObjectType::MapNode)).ThrowOnError();
        const auto& definitions = GetPipelineTableDefinitions();
        for (const auto& name : {TimerPagesTableName, TimerPageMetaTableName}) {
            CreateTable(name, GetOrCrash(definitions.Tables, std::string(name)));
        }
        CreateTable(TimerPageJournalTableName, GetOrCrash(definitions.Queues, std::string(TimerPageJournalTableName)));
        for (const auto& name : {TimerPagesTableName, TimerPageMetaTableName, TimerPageJournalTableName}) {
            WaitUntilTableMounted(YPathJoin(GetPipelinePath(), name));
        }

        Table_ = CreateTimerPages(Client_);
    }

    void TearDown() override
    {
        Table_.Reset();
        WaitForPredicate(
            [&] {
                TRemoveNodeOptions options;
                options.Force = true;
                options.Recursive = true;
                return WaitFor(Client_->RemoveNode(GetPipelinePath(), options)).IsOK();
            },
            /*iterationCount*/ 100,
            /*period*/ TDuration::MilliSeconds(200));
    }

    static TYPath GetPipelinePath()
    {
        return "//pipeline_timer_pages";
    }

    NTables::TTimerPagesPtr CreateTimerPages(
        const NApi::IClientPtr& client,
        TCountingThrottlerPtr throttler = nullptr,
        bool tinySelects = true)
    {
        auto context = New<NTables::TContext>();
        context->Client = client;
        context->PipelinePath = GetPipelinePath();
        context->LoadThroughputThrottler = New<TLoadThroughputThrottler>(
            throttler ? IReconfigurableThroughputThrottlerPtr(throttler) : CreateNamedUnlimitedThroughputThrottler("test", NProfiling::TProfiler()),
            Logger(),
            NProfiling::TProfiler());
        context->Logger = Logger();
        context->Profiler = NProfiling::TProfiler();

        // Tiny select pages exercise the continuation of every paginated read.
        auto requestSpec = New<TDynamicTableRequestSpec>();
        if (tinySelects) {
            requestSpec->SelectMinLimit = NYTree::TSize(2);
            requestSpec->SelectLimitMultiplier = 2;
            requestSpec->SelectMaxLimit = NYTree::TSize(3);
        }
        return New<NTables::TTimerPages>(context, requestSpec);
    }

    void CreateTable(TStringBuf name, const TPipelineTableDefinition& definition)
    {
        TCreateNodeOptions options;
        options.Attributes = BuildPipelineTableAttributes(definition);
        auto path = YPathJoin(GetPipelinePath(), name);
        WaitFor(Client_->CreateNode(path, EObjectType::Table, options)).ThrowOnError();
        WaitFor(Client_->MountTable(path)).ThrowOnError();
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

    template <class TAction>
    void Commit(TAction&& action)
    {
        auto tx = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet)).ValueOrThrow();
        action(tx);
        WaitFor(tx->Commit()).ThrowOnError();
    }

    static TTimer MakeTimer(ui64 key, ui64 trigger, ui64 event, ui64 system, TStringBuf messageId)
    {
        TTimer timer;
        timer.MessageId = TMessageId(messageId);
        timer.Key = MakeKey(key);
        timer.TriggerTimestamp = TSystemTimestamp(trigger);
        timer.EventTimestamp = TSystemTimestamp(event);
        timer.SystemTimestamp = TSystemTimestamp(system);
        timer.AlignmentTimestamp = timer.SystemTimestamp;
        return timer;
    }

    static TTimerPageRow MakeRow(TTimerFamilyId familyId, const TTimer& timer, bool dedupSlots)
    {
        return TTimerPageRow{
            .RowKey = MakeTimerPageRowKey(familyId, ETimerPageKind::Trigger, timer, dedupSlots),
            .Timer = timer,
        };
    }

    static void ExpectSameTimer(const TTimer& actual, const TTimer& expected)
    {
        EXPECT_EQ(actual.MessageId, expected.MessageId);
        EXPECT_EQ(actual.Key, expected.Key);
        EXPECT_EQ(actual.TriggerTimestamp, expected.TriggerTimestamp);
        EXPECT_EQ(actual.EventTimestamp, expected.EventTimestamp);
        EXPECT_EQ(actual.SystemTimestamp, expected.SystemTimestamp);
        EXPECT_EQ(actual.AlignmentTimestamp, expected.SystemTimestamp);
    }

    static TTimerPageMeta MakePage(const TComputationId& computationId, TTimerFamilyId familyId, ui64 beginTimestamp)
    {
        return TTimerPageMeta{
            .ComputationId = computationId,
            .FamilyId = familyId,
            .StreamId = TStreamId("timer"),
            .KeyRange = MakeUintKeyRange(5, std::nullopt),
            .Kind = ETimerPageKind::Trigger,
            .Begin = TSystemTimestamp(beginTimestamp),
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_W(TTestTimerPages, ConcurrentBlindMetadataUpdates)
{
    auto page = MakePage(TComputationId("comp"), TTimerFamilyId(1), 0);
    page.Count = 3;
    Commit([&] (const auto& tx) {
        Table_->WritePage(tx, page);
    });

    std::vector<ITransactionPtr> transactions;
    for (int index = 0; index < 3; ++index) {
        auto transaction = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();
        transactions.push_back(transaction);
        Table_->UpdatePage(
            transactions.back(),
            {
                .ComputationId = page.ComputationId,
                .FamilyId = page.FamilyId,
                .Begin = page.Begin,
                .Flags = {.OnceShared = index == 1 ? std::nullopt : std::optional(true)},
                .CountDelta = index == 2 ? 0 : -index - 1,
            });
    }
    std::vector<TFuture<void>> commits;
    for (const auto& transaction : transactions) {
        commits.push_back(transaction->Commit().AsVoid());
    }
    WaitFor(AllSucceeded(commits))
        .ThrowOnError();

    auto pages = WaitFor(Table_->LoadMeta({.ComputationId = page.ComputationId}))
        .ValueOrThrow();
    ASSERT_EQ(std::ssize(pages), 1);
    EXPECT_EQ(pages[0].Count, 0);
    EXPECT_TRUE(pages[0].Flags.OnceShared);
}

TEST_W(TTestTimerPages, MaxTimestampAggregatesBlindly)
{
    auto page = MakePage(TComputationId("comp"), TTimerFamilyId(1), 0);
    page.Count = 3;
    page.MaxTimestamp = TSystemTimestamp(100);
    Commit([&] (const auto& tx) {
        Table_->WritePage(tx, page);
    });

    std::vector<ITransactionPtr> transactions;
    for (int maxTimestamp : {500, 300}) {
        transactions.push_back(WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
                .ValueOrThrow());
        Table_->UpdatePage(
            transactions.back(),
            {
                .ComputationId = page.ComputationId,
                .FamilyId = page.FamilyId,
                .Begin = page.Begin,
                .CountDelta = 2,
                .MaxTimestamp = TSystemTimestamp(maxTimestamp),
            });
    }
    // The smaller maximum commits last and must not lower the stored one.
    WaitFor(transactions[0]->Commit()).ThrowOnError();
    WaitFor(transactions[1]->Commit()).ThrowOnError();

    auto pages = WaitFor(Table_->LoadMeta({.ComputationId = page.ComputationId}))
        .ValueOrThrow();
    ASSERT_EQ(std::ssize(pages), 1);
    EXPECT_EQ(pages[0].MaxTimestamp, TSystemTimestamp(500));
    EXPECT_EQ(pages[0].Count, 7);
    EXPECT_EQ(pages[0].FormatVersion, CurrentTimerPageFormatVersion);

    // A maximum alone is blind too.
    Commit([&] (const auto& tx) {
        Table_->UpdatePage(
            tx,
            {
                .ComputationId = page.ComputationId,
                .FamilyId = page.FamilyId,
                .Begin = page.Begin,
                .MaxTimestamp = TSystemTimestamp(900),
            });
    });
    pages = WaitFor(Table_->LoadMeta({.ComputationId = page.ComputationId}))
        .ValueOrThrow();
    ASSERT_EQ(std::ssize(pages), 1);
    EXPECT_EQ(pages[0].MaxTimestamp, TSystemTimestamp(900));
}

TEST_W(TTestTimerPages, NullFormatVersionAndMaxTimestampReadAsLegacy)
{
    auto page = MakePage(TComputationId("comp"), TTimerFamilyId(1), 0);
    page.FormatVersion = 1;
    page.MaxTimestamp = TSystemTimestamp(100);
    Commit([&] (const auto& tx) {
        Table_->WritePage(tx, page);
    });

    // Rows written before the columns existed have NULL in both.
    auto nameTable = New<NTableClient::TNameTable>();
    auto rowBuffer = New<NTableClient::TRowBuffer>();
    auto kind = FormatEnum(page.Kind);
    NTableClient::TUnversionedRowBuilder builder;
    builder.AddValue(rowBuffer->CaptureValue(NTableClient::MakeUnversionedStringValue(
        page.ComputationId.Underlying(),
        nameTable->RegisterName("computation_id"))));
    builder.AddValue(NTableClient::MakeUnversionedUint64Value(page.FamilyId.Underlying(), nameTable->RegisterName("family_id")));
    builder.AddValue(rowBuffer->CaptureValue(NTableClient::MakeUnversionedStringValue(kind, nameTable->RegisterName("kind"))));
    builder.AddValue(NTableClient::MakeUnversionedUint64Value(page.Begin.Underlying(), nameTable->RegisterName("begin_timestamp")));
    builder.AddValue(NTableClient::MakeUnversionedNullValue(nameTable->RegisterName("max_timestamp")));
    builder.AddValue(NTableClient::MakeUnversionedNullValue(nameTable->RegisterName("format_version")));
    std::vector<NTableClient::TUnversionedRow> rows{rowBuffer->CaptureRow(builder.GetRow())};
    Commit([&] (const auto& tx) {
        tx->WriteRows(
            YPathJoin(GetPipelinePath(), TimerPageMetaTableName),
            nameTable,
            MakeSharedRange(std::move(rows), rowBuffer));
    });

    auto pages = WaitFor(Table_->LoadMeta({.ComputationId = page.ComputationId}))
        .ValueOrThrow();
    ASSERT_EQ(std::ssize(pages), 1);
    EXPECT_EQ(pages[0].FormatVersion, 1);
    EXPECT_FALSE(pages[0].MaxTimestamp);
}

TEST_W(TTestTimerPages, LoadMetaSkipsRowsWithoutStream)
{
    const TComputationId computationId("comp");
    Commit([&] (const auto& tx) {
        for (int familyId : {1, 3, 4}) {
            Table_->WritePage(tx, MakePage(computationId, TTimerFamilyId(familyId), 0));
        }
    });
    // A blind count delta on an erased page upserts a row that has no stream; the first and
    // the last rows of the table are such ghosts too, and the selects here are tiny.
    Commit([&] (const auto& tx) {
        for (int familyId : {0, 2, 5}) {
            Table_->UpdatePage(tx, {
                    .ComputationId = computationId,
                    .FamilyId = TTimerFamilyId(familyId),
                    .Kind = ETimerPageKind::Trigger,
                    .Begin = TSystemTimestamp(0),
                    .CountDelta = 1,
                                   });
        }
    });

    auto pages = WaitFor(Table_->LoadMeta({.ComputationId = computationId})).ValueOrThrow();
    std::vector<ui64> familyIds;
    for (const auto& page : pages) {
        familyIds.push_back(page.FamilyId.Underlying());
    }
    EXPECT_EQ(familyIds, (std::vector<ui64>{1, 3, 4}));
}

TEST_W(TTestTimerPages, StructuralMetadataUpdateConflictsWithBlindWrite)
{
    std::vector<NTables::ITimerPages::TPageUpdate> updates{
        {.End = std::optional(TSystemTimestamp(100))},
        {.End = std::optional<TSystemTimestamp>{}},
        {.Minima = std::pair(TSystemTimestamp(10), TSystemTimestamp(20))},
        {.Flags = {.OnceShared = false}},
        {.Flags = {.DirtyMinima = false}},
        {.Flags = {.EventDone = false}},
        {.Flags = {.DedupSlots = false}},
    };
    int index = 0;
    for (auto update : updates) {
        SCOPED_TRACE(index);
        auto page = MakePage(TComputationId("comp"), TTimerFamilyId(++index), 0);
        page.Count = 2;
        Commit([&] (const auto& tx) {
            Table_->WritePage(tx, page);
        });

        auto structural = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();
        auto blind = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();
        update.ComputationId = page.ComputationId;
        update.FamilyId = page.FamilyId;
        update.Begin = page.Begin;
        Table_->UpdatePage(structural, update);
        Table_->UpdatePage(
            blind,
            {
                .ComputationId = page.ComputationId,
                .FamilyId = page.FamilyId,
                .Begin = page.Begin,
                .CountDelta = -1,
            });
        WaitFor(structural->Commit())
            .ThrowOnError();
        EXPECT_TRUE(WaitFor(blind->Commit()).FindMatching(NTabletClient::EErrorCode::TransactionLockConflict));

        auto pages = WaitFor(Table_->LoadMeta({.ComputationId = page.ComputationId, .FamilyId = page.FamilyId}))
            .ValueOrThrow();
        ASSERT_EQ(pages.size(), 1u);
        EXPECT_EQ(pages[0].Count, 2);
        EXPECT_EQ(pages[0].End, update.End.value_or(page.End));
    }
}

TEST_W(TTestTimerPages, ConcurrentPageRemovalCanBeRetried)
{
    auto page = MakePage(TComputationId("comp"), TTimerFamilyId(1), 0);
    page.Count = 1;
    page.Flags.OnceShared = true;
    Commit([&] (const auto& tx) {
        Table_->WritePage(tx, page);
    });

    auto decrement = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
        .ValueOrThrow();
    auto removal = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
        .ValueOrThrow();
    Table_->UpdatePage(
        decrement,
        {
            .ComputationId = page.ComputationId,
            .FamilyId = page.FamilyId,
            .Begin = page.Begin,
            .CountDelta = -1,
        });
    Table_->ErasePage(removal, page.ComputationId, page.FamilyId, page.Kind, page.Begin);
    WaitFor(decrement->Commit())
        .ThrowOnError();
    EXPECT_TRUE(WaitFor(removal->Commit()).FindMatching(NTabletClient::EErrorCode::TransactionLockConflict));

    auto pages = WaitFor(Table_->LoadMeta({.ComputationId = page.ComputationId}))
        .ValueOrThrow();
    ASSERT_EQ(std::ssize(pages), 1);
    EXPECT_EQ(pages[0].Count, 0);
    Commit([&] (const auto& tx) {
        Table_->ErasePage(tx, page.ComputationId, page.FamilyId, page.Kind, page.Begin);
    });
    EXPECT_TRUE(WaitFor(Table_->LoadMeta({.ComputationId = page.ComputationId})).ValueOrThrow().empty());
}

TEST_W(TTestTimerPages, MetadataPaginationKeepsOneSnapshot)
{
    std::vector<TTimerPageMeta> pages;
    for (int index = 0; index < 4; ++index) {
        auto page = MakePage(TComputationId("comp"), TTimerFamilyId(1), index * 10);
        page.Flags.OnceShared = true;
        if (index != 3) {
            page.End = TSystemTimestamp((index + 1) * 10);
        }
        pages.push_back(page);
    }
    Commit([&] (const auto& tx) {
        for (const auto& page : pages) {
            Table_->WritePage(tx, page);
        }
    });
    auto client = New<::testing::StrictMock<TMockClient>>();
    client->SetTimestampProvider(Client_->GetTimestampProvider());
    int calls = 0;
    EXPECT_CALL(*client, SelectRows(::testing::_, ::testing::_))
        .Times(2)
        .WillRepeatedly([&] (const std::string& query, const TSelectRowsOptions& options) {
            return Client_->SelectRows(query, options).Apply(BIND([&] (const TSelectRowsResult& result) {
                if (++calls == 1) {
                    // Collect the empty page just after its predecessor was returned.
                    Commit([&] (const auto& tx) {
                        auto extended = pages[1];
                        extended.End = pages[2].End;
                        Table_->WritePage(tx, extended);
                        Table_->ErasePage(tx, pages[2].ComputationId, pages[2].FamilyId, pages[2].Kind, pages[2].Begin);
                        Table_->WriteJournal(tx, {TTimerJournalEntry::Delete(pages[2])});
                    });
                }
                return result;
            }).AsyncVia(GetCurrentInvoker()));
        });
    auto loaded = WaitFor(CreateTimerPages(client)->LoadMeta({})).ValueOrThrow();
    ASSERT_EQ(loaded.size(), pages.size());
    for (int index = 0; index < std::ssize(pages); ++index) {
        EXPECT_EQ(loaded[index].Begin, pages[index].Begin);
        EXPECT_EQ(loaded[index].End, pages[index].End);
    }
    EXPECT_EQ(WaitFor(Table_->LoadMeta({})).ValueOrThrow().size(), 3u);
}

TEST_W(TTestTimerPages, RowsReadLookupErase)
{
    const auto familyId = TTimerFamilyId(10);
    auto page = MakePage(TComputationId("comp"), familyId, 100);
    page.End = TSystemTimestamp(107);

    // Seven rows on the page cross the select limits 2 + 3 + 3 of the paginated read; slot rows
    // and rows with an id alternate.
    std::vector<TTimerPageRow> rows;
    for (int index = 0; index < 7; ++index) {
        rows.push_back(MakeRow(familyId, MakeTimer(index / 2, 100 + index, 50 + index, 20 + index, Format("id-%v", index)), /*dedupSlots*/ index % 2 == 0));
    }
    auto otherRow = MakeRow(familyId, MakeTimer(0, 5, 5, 5, "other"), /*dedupSlots*/ false);
    auto endRow = MakeRow(familyId, MakeTimer(0, 107, 1, 1, "end"), /*dedupSlots*/ false);
    auto otherFamilyRow = MakeRow(TTimerFamilyId(11), MakeTimer(0, 103, 1, 1, "other-family"), /*dedupSlots*/ false);
    auto eventTimer = MakeTimer(0, 103, 103, 1, "event-order");
    TTimerPageRow eventRow{
        .RowKey = MakeTimerPageRowKey(familyId, ETimerPageKind::Event, eventTimer, /*dedupSlots*/ false),
        .Timer = eventTimer,
    };
    Commit([&] (const auto& tx) {
        Table_->WriteRows(tx, rows);
        Table_->WriteRows(tx, {otherRow, endRow, otherFamilyRow, eventRow});
    });

    auto readRows = WaitFor(Table_->ReadPage(page, std::nullopt)).ValueOrThrow();
    ASSERT_EQ(readRows.size(), rows.size());
    for (int index = 0; index < std::ssize(rows); ++index) {
        EXPECT_EQ(readRows[index].RowKey, rows[index].RowKey);
        ExpectSameTimer(readRows[index].Timer, rows[index].Timer);
    }

    auto statistics = WaitFor(Table_->Aggregate(page)).ValueOrThrow();
    EXPECT_EQ(statistics.Count, 7);
    EXPECT_EQ(statistics.MinTimestamp, TSystemTimestamp(100));
    EXPECT_EQ(statistics.MaxTimestamp, TSystemTimestamp(106));
    EXPECT_EQ(statistics.MinEvent, TSystemTimestamp(50));
    EXPECT_EQ(statistics.MinSystem, TSystemTimestamp(20));
    page.Count = 7;
    auto median = WaitFor(Table_->FindMedianTimestamp(page)).ValueOrThrow();
    ASSERT_EQ(median, TSystemTimestamp(103));
    auto [head, tail] = WaitFor(Table_->AggregateSplit(page, *median)).ValueOrThrow();
    EXPECT_EQ(head.Count, 3);
    EXPECT_EQ(head.MinTimestamp, TSystemTimestamp(100));
    EXPECT_EQ(head.MaxTimestamp, TSystemTimestamp(102));
    EXPECT_EQ(head.MinEvent, TSystemTimestamp(50));
    EXPECT_EQ(head.MinSystem, TSystemTimestamp(20));
    EXPECT_EQ(tail.Count, 4);
    EXPECT_EQ(tail.MinTimestamp, TSystemTimestamp(103));
    EXPECT_EQ(tail.MaxTimestamp, TSystemTimestamp(106));
    EXPECT_EQ(tail.MinEvent, TSystemTimestamp(53));
    EXPECT_EQ(tail.MinSystem, TSystemTimestamp(23));

    auto atBegin = WaitFor(Table_->AggregateSplit(page, page.Begin)).ValueOrThrow();
    EXPECT_EQ(atBegin.first.Count, 0);
    EXPECT_EQ(atBegin.second.Count, 7);
    auto atEnd = WaitFor(Table_->AggregateSplit(page, *page.End)).ValueOrThrow();
    EXPECT_EQ(atEnd.first.Count, 7);
    EXPECT_EQ(atEnd.second.Count, 0);
    auto emptyPage = page;
    emptyPage.Begin = *page.End;
    emptyPage.End = *page.End;
    emptyPage.Count = 0;
    EXPECT_FALSE(WaitFor(Table_->FindMedianTimestamp(emptyPage)).ValueOrThrow());

    auto rangeRows = WaitFor(Table_->ReadPage(page, MakeUintKeyRange(1, 3))).ValueOrThrow();
    ASSERT_EQ(rangeRows.size(), 4u);
    EXPECT_EQ(rangeRows.front().Timer.MessageId, TMessageId("id-2"));
    EXPECT_EQ(rangeRows.back().Timer.MessageId, TMessageId("id-5"));

    auto missingKey = rows[1].RowKey;
    missingKey.RowId = TMessageId("absent");
    auto looked = WaitFor(Table_->LookupRows({rows[0].RowKey, missingKey, rows[3].RowKey, otherRow.RowKey})).ValueOrThrow();
    ASSERT_EQ(looked.size(), 4u);
    ASSERT_TRUE(looked[0]);
    EXPECT_TRUE(looked[0]->RowKey.RowId.Underlying().empty());
    ExpectSameTimer(looked[0]->Timer, rows[0].Timer);
    EXPECT_FALSE(looked[1]);
    ASSERT_TRUE(looked[2]);
    EXPECT_EQ(looked[2]->Timer.EventTimestamp, TSystemTimestamp(53));
    ASSERT_TRUE(looked[3]);
    EXPECT_EQ(looked[3]->Timer.MessageId, TMessageId("other"));

    Commit([&] (const auto& tx) {
        Table_->EraseRows(tx, {rows[0].RowKey, rows[6].RowKey});
    });
    auto afterErase = WaitFor(Table_->ReadPage(page, std::nullopt)).ValueOrThrow();
    ASSERT_EQ(afterErase.size(), 5u);
    EXPECT_EQ(afterErase.front().Timer.MessageId, TMessageId("id-1"));
    EXPECT_EQ(afterErase.back().Timer.MessageId, TMessageId("id-5"));
}

TEST_W(TTestTimerPages, ReadPageByExactKeyRange)
{
    const auto familyId = TTimerFamilyId(12);
    auto page = MakePage(TComputationId("comp"), familyId, 100);
    page.End = TSystemTimestamp(110);

    // Keys 0, 0, 1, 1, 2, 2 interleave in time, so the neighbours of key 1 surround its rows.
    std::vector<TTimerPageRow> rows;
    for (int index = 0; index < 6; ++index) {
        rows.push_back(MakeRow(familyId, MakeTimer(index / 2, 100 + index, 50 + index, 20 + index, Format("id-%v", index)), /*dedupSlots*/ false));
    }
    Commit([&] (const auto& tx) {
        Table_->WriteRows(tx, rows);
    });

    // The range of a source-key partition: the key followed by a Max sentinel.
    auto lower = MakeKey(ui64(1));
    NTableClient::TUnversionedOwningRowBuilder builder;
    for (auto value : lower.Underlying().Get()) {
        builder.AddValue(value);
    }
    builder.AddValue(NTableClient::MakeUnversionedSentinelValue(NTableClient::EValueType::Max));
    TKeyRange exactRange{
        .Lower = lower,
        .Upper = TKey(TCompactUnversionedOwningRow(builder.FinishRow())),
    };
    auto exactRows = WaitFor(Table_->ReadPage(page, exactRange)).ValueOrThrow();
    ASSERT_EQ(exactRows.size(), 2u);
    EXPECT_EQ(exactRows[0].Timer.MessageId, TMessageId("id-2"));
    EXPECT_EQ(exactRows[1].Timer.MessageId, TMessageId("id-3"));
}

TEST_W(TTestTimerPages, ReadPageUsesOneSelectForKnownCount)
{
    const auto familyId = TTimerFamilyId(10);
    constexpr int RowCount = 300;
    auto page = MakePage(TComputationId("comp"), familyId, 100);
    page.End = TSystemTimestamp(100 + RowCount);

    std::vector<TTimerPageRow> rows;
    for (int index = 0; index < RowCount; ++index) {
        rows.push_back(MakeRow(familyId, MakeTimer(index, 100 + index, 50, 20, Format("id-%v", index)), /*dedupSlots*/ false));
    }
    Commit([&] (const auto& tx) {
        Table_->WriteRows(tx, rows);
    });

    auto throttler = New<TCountingThrottler>(CreateNamedUnlimitedThroughputThrottler("test", NProfiling::TProfiler()));
    auto table = CreateTimerPages(Client_, throttler, /*tinySelects*/ false);

    page.Count = RowCount;
    auto readRows = WaitFor(table->ReadPage(page, std::nullopt)).ValueOrThrow();
    ASSERT_EQ(std::ssize(readRows), RowCount);
    for (int index = 0; index < RowCount; ++index) {
        EXPECT_EQ(readRows[index].RowKey, rows[index].RowKey);
        ExpectSameTimer(readRows[index].Timer, rows[index].Timer);
    }
    EXPECT_EQ(throttler->GetThrottleCount(), 1);

    // An understated count still yields all rows, through continuation selects: 101 rows by the
    // count, then 50 and 250 by the limiter.
    page.Count = 100;
    i64 beforeCount = throttler->GetThrottleCount();
    readRows = WaitFor(table->ReadPage(page, std::nullopt)).ValueOrThrow();
    ASSERT_EQ(std::ssize(readRows), RowCount);
    for (int index = 0; index < RowCount; ++index) {
        EXPECT_EQ(readRows[index].RowKey, rows[index].RowKey);
    }
    EXPECT_EQ(throttler->GetThrottleCount() - beforeCount, 3);

    // A key-filtered read of a shared page ignores the count: the limiter alone sizes the selects.
    page.Count = RowCount;
    beforeCount = throttler->GetThrottleCount();
    readRows = WaitFor(table->ReadPage(page, UniversalKeyRange())).ValueOrThrow();
    ASSERT_EQ(std::ssize(readRows), RowCount);
    EXPECT_EQ(throttler->GetThrottleCount() - beforeCount, 3);
}

TEST_W(TTestTimerPages, MaintenanceIgnoresDefaultRowLimits)
{
    const auto familyId = TTimerFamilyId(20);
    auto page = MakePage(TComputationId("comp"), familyId, 100);
    std::vector<TTimerPageRow> rows;
    for (int index = 0; index < 7; ++index) {
        rows.push_back(MakeRow(familyId, MakeTimer(index, 100 + index, 50 + index, 20 + index, Format("id-%v", index)), /*dedupSlots*/ false));
    }
    Commit([&] (const auto& tx) {
        Table_->WriteRows(tx, rows);
    });

    auto config = NYTree::ConvertTo<NApi::NNative::TConnectionCompoundConfigPtr>(
        WaitFor(Client_->GetNode("//sys/@cluster_connection")).ValueOrThrow());
    config->Dynamic->DefaultInputRowLimit = 2;
    config->Dynamic->DefaultOutputRowLimit = 2;
    auto connection = NApi::NNative::CreateConnection(std::move(config));
    auto terminateConnection = Finally([&] {
        connection->Terminate();
    });
    auto client = connection->CreateClient(TClientOptions::FromUser("root"));
    auto query = Format("sum(1) from [%v] group by 0 as bucket", YPathJoin(GetPipelinePath(), TimerPagesTableName));
    auto incomplete = WaitFor(client->SelectRows(query));
    ASSERT_TRUE(incomplete.FindMatching(NTabletClient::EErrorCode::QueryInputRowCountLimitExceeded));

    auto table = CreateTimerPages(client);
    auto statistics = WaitFor(table->Aggregate(page)).ValueOrThrow();
    EXPECT_EQ(statistics.Count, 7);
    EXPECT_EQ(statistics.MinTimestamp, TSystemTimestamp(100));
    EXPECT_EQ(statistics.MaxTimestamp, TSystemTimestamp(106));
    page.Count = 7;
    EXPECT_EQ(WaitFor(table->FindMedianTimestamp(page)).ValueOrThrow(), TSystemTimestamp(103));
    auto [head, tail] = WaitFor(table->AggregateSplit(page, TSystemTimestamp(103))).ValueOrThrow();
    auto total = head;
    total.Add(tail);
    EXPECT_EQ(total.Count, 7);
    EXPECT_EQ(total.MinEvent, TSystemTimestamp(50));
    EXPECT_EQ(total.MinSystem, TSystemTimestamp(20));
}

TEST_W(TTestTimerPages, SlotRowReplacement)
{
    const auto familyId = TTimerFamilyId(20);
    auto page = MakePage(TComputationId("comp"), familyId, 100);

    // Two timers of one dedup slot share a row key: the store replaces the loser by erasing and
    // writing the key within one transaction, which must leave the new row.
    auto older = MakeRow(familyId, MakeTimer(1, 100, 40, 70, "older"), /*dedupSlots*/ true);
    auto newer = MakeRow(familyId, MakeTimer(1, 100, 60, 10, "newer"), /*dedupSlots*/ true);
    ASSERT_EQ(older.RowKey, newer.RowKey);

    Commit([&] (const auto& tx) {
        Table_->WriteRows(tx, {older});
    });
    Commit([&] (const auto& tx) {
        Table_->EraseRows(tx, {older.RowKey});
        Table_->WriteRows(tx, {newer});
    });
    auto rows = WaitFor(Table_->ReadPage(page, std::nullopt)).ValueOrThrow();
    ASSERT_EQ(rows.size(), 1u);
    ExpectSameTimer(rows[0].Timer, newer.Timer);

    // And the other order within one transaction erases the row for good.
    Commit([&] (const auto& tx) {
        Table_->WriteRows(tx, {older});
        Table_->EraseRows(tx, {older.RowKey});
    });
    EXPECT_TRUE(WaitFor(Table_->ReadPage(page, std::nullopt)).ValueOrThrow().empty());
}

TEST_W(TTestTimerPages, Meta)
{
    const auto computationId = TComputationId("comp");
    const auto familyId = TTimerFamilyId(30);

    std::vector<TTimerPageMeta> pages;
    for (int index = 1; index <= 4; ++index) {
        auto page = MakePage(computationId, familyId, 30 + index);
        page.Begin = TSystemTimestamp(index == 1 ? 0 : 100 * index);
        page.End = index == 4 ? std::nullopt : std::optional(TSystemTimestamp(100 * (index + 1)));
        page.Flags.DedupSlots = index % 2 == 0;
        page.Count = static_cast<i64>(10 * index);
        page.MinEvent = TSystemTimestamp(1000 + index);
        page.MinSystem = TSystemTimestamp(2000 + index);
        page.MaxTimestamp = index % 2 == 0 ? std::optional(TSystemTimestamp(3000 + index)) : std::nullopt;
        pages.push_back(page);
    }
    auto otherPage = MakePage(TComputationId("other"), TTimerFamilyId(31), 32);
    otherPage.KeyRange = UniversalKeyRange();
    otherPage.Flags.OnceShared = true;
    auto otherFamilyPage = MakePage(computationId, TTimerFamilyId(33), 34);

    Commit([&] (const auto& tx) {
        for (const auto& page : pages) {
            Table_->WritePage(tx, page);
        }
        Table_->WritePage(tx, otherPage);
        Table_->WritePage(tx, otherFamilyPage);
    });

    auto meta = WaitFor(Table_->LoadMeta({.ComputationId = computationId, .FamilyId = familyId})).ValueOrThrow();
    ASSERT_EQ(meta.size(), pages.size());
    for (int index = 0; index < std::ssize(pages); ++index) {
        EXPECT_EQ(meta[index], pages[index]) << index;
    }

    auto all = WaitFor(Table_->LoadMeta({.ComputationId = computationId})).ValueOrThrow();
    ASSERT_EQ(all.size(), pages.size() + 1);
    EXPECT_EQ(all.back(), otherFamilyPage);

    auto other = WaitFor(Table_->LoadMeta({.ComputationId = otherPage.ComputationId})).ValueOrThrow();
    ASSERT_EQ(other.size(), 1u);
    EXPECT_EQ(other[0], otherPage);

    // Two count deltas in separate transactions add up; the other columns stay.
    auto& page = pages[1];
    for (i64 delta : {3, -5}) {
        Commit([&] (const auto& tx) {
            Table_->UpdatePage(tx, {.ComputationId = computationId, .FamilyId = familyId, .Begin = page.Begin, .CountDelta = delta});
        });
    }
    page.Count -= 2;

    // Two writers of a shared page change different flags in separate transactions without
    // clobbering each other.
    Commit([&] (const auto& tx) {
        Table_->UpdatePage(tx, {
                .ComputationId = computationId,
                .FamilyId = familyId,
                .Begin = page.Begin,
                .Flags = {.DirtyMinima = true},
                .Minima = std::pair(TSystemTimestamp(3), TSystemTimestamp(4)),
                               });
    });
    Commit([&] (const auto& tx) {
        Table_->UpdatePage(tx, {
                .ComputationId = computationId,
                .FamilyId = familyId,
                .Begin = page.Begin,
                .Flags = {.OnceShared = true},
                               });
    });
    page.Flags.DirtyMinima = true;
    page.Flags.OnceShared = true;
    page.MinEvent = TSystemTimestamp(3);
    page.MinSystem = TSystemTimestamp(4);
    Commit([&] (const auto& tx) {
        Table_->UpdatePage(tx, {
                .ComputationId = computationId,
                .FamilyId = familyId,
                .Begin = page.Begin,
                .End = std::make_optional(pages[3].End),
                               });
    });
    page.End = pages[3].End;

    meta = WaitFor(Table_->LoadMeta({.ComputationId = computationId, .FamilyId = familyId})).ValueOrThrow();
    ASSERT_EQ(meta.size(), pages.size());
    EXPECT_EQ(meta[1], page);
    EXPECT_EQ(meta[0], pages[0]);

    Commit([&] (const auto& tx) {
        Table_->ErasePage(tx, computationId, familyId, pages[0].Kind, pages[0].Begin);
    });
    meta = WaitFor(Table_->LoadMeta({.ComputationId = computationId, .FamilyId = familyId})).ValueOrThrow();
    ASSERT_EQ(meta.size(), pages.size() - 1);
    EXPECT_EQ(meta[0], page);
}

TEST_W(TTestTimerPages, Journal)
{
    auto range = WaitFor(Table_->GetJournalRowRange()).ValueOrThrow();
    EXPECT_EQ(range.TrimmedRowCount, 0);
    EXPECT_EQ(range.TotalRowCount, 0);

    const auto computationId = TComputationId("comp");
    const auto familyId = TTimerFamilyId(40);
    std::vector<TTimerJournalEntry> entries;
    for (int index = 1; index <= 3; ++index) {
        auto page = MakePage(computationId, familyId, 40 + index);
        page.KeyRange = MakeUintKeyRange(std::nullopt, 7);
        page.Kind = index == 3 ? ETimerPageKind::Event : ETimerPageKind::Trigger;
        page.Flags.EventDone = index == 3;
        page.Flags.DedupSlots = index == 1;
        // Private flags never reach the journal.
        page.Flags.DirtyMinima = true;
        entries.push_back(TTimerJournalEntry::Create(page));
    }
    auto page = MakePage(computationId, familyId, 41);
    page.KeyRange = MakeUintKeyRange(std::nullopt, 7);
    entries.push_back(TTimerJournalEntry::SetFlags(page, {.OnceShared = true}));
    entries.push_back(TTimerJournalEntry::Delete(page));

    Commit([&] (const auto& tx) {
        Table_->WriteJournal(tx, {entries[0], entries[1]});
    });
    Commit([&] (const auto& tx) {
        Table_->WriteJournal(tx, {entries.begin() + 2, entries.end()});
    });

    range = WaitFor(Table_->GetJournalRowRange()).ValueOrThrow();
    EXPECT_EQ(range.TrimmedRowCount, 0);
    EXPECT_EQ(range.TotalRowCount, std::ssize(entries));

    std::vector<TTimerJournalEntry> readEntries;
    i64 rowIndex = 0;
    while (rowIndex < range.TotalRowCount) {
        auto result = WaitFor(Table_->ReadJournal(rowIndex, /*limit*/ 4)).ValueOrThrow();
        ASSERT_FALSE(result.Entries.empty());
        EXPECT_EQ(result.NextRowIndex, rowIndex + std::ssize(result.Entries));
        readEntries.insert(readEntries.end(), result.Entries.begin(), result.Entries.end());
        rowIndex = result.NextRowIndex;
    }
    ASSERT_EQ(readEntries.size(), entries.size());
    for (int index = 0; index < std::ssize(entries); ++index) {
        EXPECT_EQ(readEntries[index].RowIndex, index);
        auto expected = entries[index];
        expected.RowIndex = index;
        EXPECT_EQ(readEntries[index], expected) << index;
    }
    EXPECT_FALSE(readEntries[0].Flags.DirtyMinima);
    EXPECT_EQ(readEntries[0].Flags.DedupSlots, std::optional(true));
    EXPECT_EQ(readEntries[3].Flags.OnceShared, std::optional(true));
    EXPECT_FALSE(readEntries[3].Flags.DedupSlots);
    EXPECT_TRUE(readEntries[4].Flags.IsEmpty());

    auto tail = WaitFor(Table_->ReadJournal(range.TotalRowCount, /*limit*/ 4)).ValueOrThrow();
    EXPECT_TRUE(tail.Entries.empty());
    EXPECT_EQ(tail.NextRowIndex, range.TotalRowCount);
}

TEST_W(TTestTimerPages, TablesExist)
{
    EXPECT_TRUE(WaitFor(Table_->TablesExist()).ValueOrThrow());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
