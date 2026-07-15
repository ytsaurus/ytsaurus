#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/state.h>

#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>
#include <yt/yt/flow/library/cpp/computation/static_table_key_visitor_joiner.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/cache/cache.h>

#include <yt/yt/client/unittests/mock/client.h>

#include <yt/yt/client/api/table_reader.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/ytree/convert.h>

#include <deque>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TStubClientsCache
    : public NClient::NCache::IClientsCache
{
public:
    NApi::IClientPtr GetClient(TStringBuf /*clusterUrl*/) override
    {
        return New<NApi::TMockClient>();
    }
};

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr MakeKeySchema()
{
    return New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("hash", EValueType::Uint64, ESortOrder::Ascending),
        TColumnSchema("key", EValueType::String, ESortOrder::Ascending),
    });
}

TTableSchemaPtr MakePayloadSchema()
{
    return New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("a", EValueType::Int64),
        TColumnSchema("b", EValueType::String),
    });
}

TKey MakeRowKey(ui64 hash, TStringBuf key)
{
    return MakeKey(hash, key);
}

TPayload MakePayload(const TTableSchemaPtr& schema, i64 a, TStringBuf b)
{
    TPayloadBuilder builder(schema);
    builder.Set<i64>(a, "a");
    builder.Set<TStringBuf>(b, "b");
    return builder.Finish();
}

////////////////////////////////////////////////////////////////////////////////

class TFakeStaticTableKeyVisitorJoiner
    : public TStaticTableKeyVisitorJoiner
{
public:
    using TListedStates = TStaticTableKeyVisitorJoiner::TListedStates;

    TFakeStaticTableKeyVisitorJoiner(
        TExternalStateJoinerContextPtr context,
        TDynamicExternalStateJoinerContextPtr dynamicContext)
        : TStaticTableKeyVisitorJoiner(std::move(context), std::move(dynamicContext))
    { }

    void ScriptListResult(TListedStates result)
    {
        ScriptedResults_.push_back(std::move(result));
    }

    void ScriptListFailure(TError error)
    {
        ScriptedError_ = std::move(error);
    }

    int ListedSize() const
    {
        return std::ssize(Listed_);
    }

    int PendingScriptedResults() const
    {
        return std::ssize(ScriptedResults_);
    }

protected:
    TFuture<TListedStates> DoListStates(TFilter /*filter*/, i64 /*limit*/) override
    {
        if (!ScriptedError_.IsOK()) {
            return MakeFuture<TListedStates>(std::exchange(ScriptedError_, TError()));
        }
        YT_VERIFY(!ScriptedResults_.empty());
        auto result = std::move(ScriptedResults_.front());
        ScriptedResults_.pop_front();
        return MakeFuture(std::move(result));
    }

private:
    std::deque<TListedStates> ScriptedResults_;
    TError ScriptedError_;
};

DEFINE_REFCOUNTED_TYPE(TFakeStaticTableKeyVisitorJoiner);

////////////////////////////////////////////////////////////////////////////////

class TStaticTableKeyVisitorJoinerTest
    : public ::testing::Test
{
protected:
    const TTableSchemaPtr KeySchema_ = MakeKeySchema();
    const TTableSchemaPtr PayloadSchema_ = MakePayloadSchema();
    const TActionQueuePtr Queue_ = New<TActionQueue>("KeyVisitorJoinerTest");

    TExternalStateJoinerContextPtr MakeContext(EUnavailableSourcePolicy policy)
    {
        auto context = New<TExternalStateJoinerContext>();
        context->KeySchema = KeySchema_;
        context->ClientsCache = New<TStubClientsCache>();
        context->SerializedInvoker = Queue_->GetInvoker();
        context->StatusProfiler = CreateSyncStatusProfiler();
        context->PipelinePath = NYPath::TRichYPath("//pipeline");
        context->PipelinePath.SetCluster("test");
        context->Logger = NLogging::TLogger("Test");

        auto spec = New<TExternalStateJoinerSpec>();
        spec->ExternalStateJoinerClassName = "NYT::NFlow::TStaticTableKeyVisitorJoiner";
        spec->JoinOn = New<TStateJoinSpec>();
        spec->Parameters = BuildYsonNodeFluently()
            .BeginMap()
            .Item("path")
            .Value("//table")
            .Item("unavailable_source_policy")
            .Value(FormatEnum(policy))
            .EndMap()
            ->AsMap();
        context->ExternalStateJoinerSpec = std::move(spec);
        return context;
    }

    static TDynamicExternalStateJoinerContextPtr MakeDynamicContext()
    {
        auto dynamicContext = New<TDynamicExternalStateJoinerContext>();
        dynamicContext->DynamicExternalStateJoinerSpec = New<TDynamicExternalStateJoinerSpec>();
        return dynamicContext;
    }

    TIntrusivePtr<TFakeStaticTableKeyVisitorJoiner> MakeJoiner(
        EUnavailableSourcePolicy policy = EUnavailableSourcePolicy::Retry)
    {
        return New<TFakeStaticTableKeyVisitorJoiner>(MakeContext(policy), MakeDynamicContext());
    }

    TFakeStaticTableKeyVisitorJoiner::TListedStates MakeListedStates(
        std::vector<std::tuple<TKey, i64, TStringBuf>> rows)
    {
        TFakeStaticTableKeyVisitorJoiner::TListedStates listed;
        listed.StateSchema = PayloadSchema_;
        for (const auto& [key, a, b] : rows) {
            listed.Keys.push_back(key);
            listed.Payloads.push_back(MakePayload(PayloadSchema_, a, b));
        }
        return listed;
    }

    static const TSimpleExternalState& AsSimpleState(const IStateHolderPtr& holder)
    {
        auto typed = DynamicPointerCast<TStateHolder<TSimpleExternalState>>(holder);
        YT_VERIFY(typed);
        return typed->Get();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TStaticTableKeyVisitorJoinerTest, IsVisitorDriven)
{
    auto joiner = MakeJoiner();
    EXPECT_TRUE(joiner->IsVisitorDriven());
}

TEST_F(TStaticTableKeyVisitorJoinerTest, ListPopulatesListed)
{
    auto joiner = MakeJoiner();

    auto k1 = MakeRowKey(1, "a");
    auto k2 = MakeRowKey(1, "b");
    joiner->ScriptListResult(MakeListedStates({{k1, 10, "x"}, {k2, 20, "y"}}));

    auto result = WaitFor(joiner->List(/*filter*/ {}, /*limit*/ 100, /*offsetExclusive*/ std::nullopt))
        .ValueOrThrow();

    EXPECT_EQ(std::ssize(result.Keys), 2);
    EXPECT_EQ(joiner->ListedSize(), 2);
}

TEST_F(TStaticTableKeyVisitorJoinerTest, PreloadPromotesPresentKey)
{
    auto joiner = MakeJoiner();

    auto k1 = MakeRowKey(1, "a");
    joiner->ScriptListResult(MakeListedStates({{k1, 10, "x"}}));
    WaitFor(joiner->List({}, 100, std::nullopt)).ValueOrThrow();

    WaitFor(joiner->PreloadKeyStates({k1})).ThrowOnError();

    auto holder = joiner->GetState(k1);
    EXPECT_FALSE(holder->IsEmpty());
    const auto& state = AsSimpleState(holder);
    EXPECT_EQ(state.GetColumnValue<i64>("a"), 10);
    EXPECT_EQ(state.GetColumnValue<std::string>("b"), "x");
}

TEST_F(TStaticTableKeyVisitorJoinerTest, ConsumeDropsListedRowKeepsReadAhead)
{
    auto joiner = MakeJoiner();

    auto consumed = MakeRowKey(1, "a");
    auto readAhead = MakeRowKey(1, "b");
    joiner->ScriptListResult(MakeListedStates({{consumed, 10, "x"}, {readAhead, 20, "y"}}));
    WaitFor(joiner->List({}, 100, std::nullopt)).ValueOrThrow();
    EXPECT_EQ(joiner->ListedSize(), 2);

    WaitFor(joiner->PreloadKeyStates({consumed})).ThrowOnError();
    EXPECT_EQ(joiner->ListedSize(), 1);

    joiner->Reset();

    EXPECT_EQ(joiner->ListedSize(), 1);

    WaitFor(joiner->PreloadKeyStates({readAhead})).ThrowOnError();
    EXPECT_FALSE(joiner->GetState(readAhead)->IsEmpty());
    EXPECT_EQ(AsSimpleState(joiner->GetState(readAhead)).GetColumnValue<i64>("a"), 20);
}

TEST_F(TStaticTableKeyVisitorJoinerTest, FailedListPropagatesUnderRetryPolicy)
{
    auto joiner = MakeJoiner(EUnavailableSourcePolicy::Retry);

    joiner->ScriptListFailure(TError("source cluster gone"));

    auto result = WaitFor(joiner->List({}, 100, std::nullopt));
    EXPECT_FALSE(result.IsOK());
}

TEST_F(TStaticTableKeyVisitorJoinerTest, FailedListSwallowedWhenMarkUnreadable)
{
    auto joiner = MakeJoiner(EUnavailableSourcePolicy::MarkUnreadable);

    auto unreadable = MakeRowKey(1, "a");
    joiner->ScriptListFailure(TError("source cluster gone"));

    auto result = WaitFor(joiner->List({}, 100, std::nullopt)).ValueOrThrow();
    EXPECT_TRUE(result.Keys.empty());

    WaitFor(joiner->PreloadKeyStates({unreadable})).ThrowOnError();
    EXPECT_EQ(joiner->GetState(unreadable), nullptr);
}

TEST_F(TStaticTableKeyVisitorJoinerTest, ListedRangeAbsentKeyIsEmptyNotUnreadable)
{
    auto joiner = MakeJoiner(EUnavailableSourcePolicy::MarkUnreadable);

    auto present = MakeRowKey(1, "a");
    auto absent = MakeRowKey(1, "z");
    joiner->ScriptListResult(MakeListedStates({{present, 10, "x"}}));
    WaitFor(joiner->List({}, 100, std::nullopt)).ValueOrThrow();

    WaitFor(joiner->PreloadKeyStates({present, absent})).ThrowOnError();

    EXPECT_NE(joiner->GetState(present), nullptr);
    EXPECT_FALSE(joiner->GetState(present)->IsEmpty());
    EXPECT_NE(joiner->GetState(absent), nullptr);
    EXPECT_TRUE(joiner->GetState(absent)->IsEmpty());
}

TEST_F(TStaticTableKeyVisitorJoinerTest, ResetKeepsKeyRelistedByNewerPass)
{
    auto joiner = MakeJoiner();

    auto k = MakeRowKey(1, "a");
    joiner->ScriptListResult(MakeListedStates({{k, 10, "v1"}}));
    WaitFor(joiner->List({}, 100, std::nullopt)).ValueOrThrow();
    WaitFor(joiner->PreloadKeyStates({k})).ThrowOnError();

    joiner->ScriptListResult(MakeListedStates({{k, 20, "v2"}}));
    WaitFor(joiner->List({}, 100, std::nullopt)).ValueOrThrow();

    joiner->Reset();

    EXPECT_EQ(joiner->ListedSize(), 1);
    WaitFor(joiner->PreloadKeyStates({k})).ThrowOnError();
    auto holder = joiner->GetState(k);
    EXPECT_FALSE(holder->IsEmpty());
    EXPECT_EQ(AsSimpleState(holder).GetColumnValue<i64>("a"), 20);
    EXPECT_EQ(AsSimpleState(holder).GetColumnValue<std::string>("b"), "v2");
}

TEST_F(TStaticTableKeyVisitorJoinerTest, GetStateThrowsForNonPreloadedKey)
{
    auto joiner = MakeJoiner();

    auto k = MakeRowKey(1, "a");
    joiner->ScriptListResult(MakeListedStates({{k, 10, "x"}}));
    WaitFor(joiner->List({}, 100, std::nullopt)).ValueOrThrow();

    auto other = MakeRowKey(2, "b");
    EXPECT_THROW(joiner->GetState(other), std::exception);
}

TEST_F(TStaticTableKeyVisitorJoinerTest, BackoffFailsFastWithoutTouchingSource)
{
    auto joiner = MakeJoiner(EUnavailableSourcePolicy::Retry);

    joiner->ScriptListFailure(TError("source cluster gone"));
    EXPECT_FALSE(WaitFor(joiner->List({}, 100, std::nullopt)).IsOK());

    auto k = MakeRowKey(1, "a");
    joiner->ScriptListResult(MakeListedStates({{k, 10, "x"}}));
    EXPECT_FALSE(WaitFor(joiner->List({}, 100, std::nullopt)).IsOK());
    EXPECT_EQ(joiner->PendingScriptedResults(), 1);
    EXPECT_EQ(joiner->ListedSize(), 0);
}

TEST_F(TStaticTableKeyVisitorJoinerTest, BackoffMarksRangeUnreadableWithoutTouchingSource)
{
    auto joiner = MakeJoiner(EUnavailableSourcePolicy::MarkUnreadable);

    auto key = MakeRowKey(1, "z");
    joiner->ScriptListFailure(TError("source cluster gone"));
    WaitFor(joiner->List({}, 100, std::nullopt)).ValueOrThrow();

    joiner->ScriptListResult(MakeListedStates({}));
    auto result = WaitFor(joiner->List({}, 100, std::nullopt)).ValueOrThrow();
    EXPECT_TRUE(result.Keys.empty());
    EXPECT_EQ(joiner->PendingScriptedResults(), 1);

    WaitFor(joiner->PreloadKeyStates({key})).ThrowOnError();
    EXPECT_EQ(joiner->GetState(key), nullptr);
}

TEST_F(TStaticTableKeyVisitorJoinerTest, CoverageSurvivesResetForLateVisits)
{
    auto joiner = MakeJoiner();

    auto consumed = MakeRowKey(1, "a");
    auto absentLate = MakeRowKey(1, "z");
    joiner->ScriptListResult(MakeListedStates({{consumed, 10, "x"}}));
    WaitFor(joiner->List({}, 100, std::nullopt)).ValueOrThrow();
    WaitFor(joiner->PreloadKeyStates({consumed})).ThrowOnError();

    joiner->Reset();

    WaitFor(joiner->PreloadKeyStates({absentLate})).ThrowOnError();
    EXPECT_NE(joiner->GetState(absentLate), nullptr);
    EXPECT_TRUE(joiner->GetState(absentLate)->IsEmpty());
}

TEST_F(TStaticTableKeyVisitorJoinerTest, FailedRelistRetractsCoverage)
{
    auto joiner = MakeJoiner(EUnavailableSourcePolicy::MarkUnreadable);

    auto key = MakeRowKey(1, "z");
    joiner->ScriptListResult(MakeListedStates({}));
    WaitFor(joiner->List({}, 100, std::nullopt)).ValueOrThrow();

    WaitFor(joiner->PreloadKeyStates({key})).ThrowOnError();
    EXPECT_NE(joiner->GetState(key), nullptr);
    EXPECT_TRUE(joiner->GetState(key)->IsEmpty());

    joiner->Reset();
    joiner->ScriptListFailure(TError("source cluster gone"));
    WaitFor(joiner->List({}, 100, std::nullopt)).ValueOrThrow();

    WaitFor(joiner->PreloadKeyStates({key})).ThrowOnError();
    EXPECT_EQ(joiner->GetState(key), nullptr);
}

TEST_F(TStaticTableKeyVisitorJoinerTest, CapTruncatedListLeavesTailUnreadable)
{
    auto joiner = MakeJoiner(EUnavailableSourcePolicy::MarkUnreadable);

    auto k1 = MakeRowKey(1, "a");
    auto k2 = MakeRowKey(1, "b");
    auto beyond = MakeRowKey(1, "c");
    joiner->ScriptListResult(MakeListedStates({{k1, 10, "x"}, {k2, 20, "y"}}));
    WaitFor(joiner->List({}, /*limit*/ 2, std::nullopt)).ValueOrThrow();

    WaitFor(joiner->PreloadKeyStates({k1, beyond})).ThrowOnError();

    EXPECT_FALSE(joiner->GetState(k1)->IsEmpty());
    EXPECT_EQ(joiner->GetState(beyond), nullptr);
}

////////////////////////////////////////////////////////////////////////////////

//! Serves scripted rows through the ITableReader interface, mimicking a real forward reader:
//! rows come in ascending key order starting from the requested lower bound.
class TFakeTableReader
    : public NApi::ITableReader
{
public:
    TFakeTableReader(
        TNameTablePtr nameTable,
        TTableSchemaPtr schema,
        std::vector<TUnversionedOwningRow> rows,
        int leadingEmptyBatches)
        : NameTable_(std::move(nameTable))
        , Schema_(std::move(schema))
        , Rows_(std::move(rows))
        , LeadingEmptyBatches_(leadingEmptyBatches)
    { }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        if (LeadingEmptyBatches_ > 0) {
            --LeadingEmptyBatches_;
            return CreateEmptyUnversionedRowBatch();
        }
        if (Position_ >= std::ssize(Rows_)) {
            return nullptr;
        }
        auto count = std::min<i64>(options.MaxRowsPerRead, std::ssize(Rows_) - Position_);
        auto holder = std::make_shared<std::vector<TUnversionedOwningRow>>(
            Rows_.begin() + Position_,
            Rows_.begin() + Position_ + count);
        std::vector<TUnversionedRow> batchRows;
        batchRows.reserve(count);
        for (const auto& row : *holder) {
            batchRows.push_back(row);
        }
        Position_ += count;
        return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(batchRows), std::move(holder)));
    }

    TFuture<void> GetReadyEvent() const override
    {
        return OKFuture;
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    i64 GetStartRowIndex() const override
    {
        return 0;
    }

    i64 GetTotalRowCount() const override
    {
        return std::ssize(Rows_);
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return {};
    }

    const TTableSchemaPtr& GetTableSchema() const override
    {
        return Schema_;
    }

    const std::vector<std::string>& GetOmittedInaccessibleColumns() const override
    {
        static const std::vector<std::string> empty;
        return empty;
    }

private:
    const TNameTablePtr NameTable_;
    const TTableSchemaPtr Schema_;
    const std::vector<TUnversionedOwningRow> Rows_;

    int LeadingEmptyBatches_ = 0;
    i64 Position_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TFixedClientsCache
    : public NClient::NCache::IClientsCache
{
public:
    explicit TFixedClientsCache(NApi::IClientPtr client)
        : Client_(std::move(client))
    { }

    NApi::IClientPtr GetClient(TStringBuf /*clusterUrl*/) override
    {
        return Client_;
    }

private:
    const NApi::IClientPtr Client_;
};

////////////////////////////////////////////////////////////////////////////////

//! Exercises the real #DoListStates forward cursor over a fake table reader.
class TStaticTableJoinerCursorTest
    : public ::testing::Test
{
protected:
    const TTableSchemaPtr KeySchema_ = MakeKeySchema();
    const TActionQueuePtr Queue_ = New<TActionQueue>("JoinerCursorTest");
    const TIntrusivePtr<NApi::TMockClient> Mock_ = New<NApi::TMockClient>();

    //! The reader name table deliberately scrambles the column order relative to the schema.
    const TNameTablePtr ReaderNameTable_ = New<TNameTable>();
    const int BId_ = ReaderNameTable_->RegisterName("b");
    const int HashId_ = ReaderNameTable_->RegisterName("hash");
    const int AId_ = ReaderNameTable_->RegisterName("a");
    const int KeyId_ = ReaderNameTable_->RegisterName("key");

    const TTableSchemaPtr TableSchema_ = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("hash", EValueType::Uint64, ESortOrder::Ascending),
        TColumnSchema("key", EValueType::String, ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64),
        TColumnSchema("b", EValueType::String),
    });

    struct TSourceRow
    {
        TKey Key;
        TUnversionedOwningRow Row;
    };

    std::vector<TSourceRow> SourceRows_;
    int LeadingEmptyBatches_ = 0;
    std::vector<TKey> ReaderOpens_;

    void AddSourceRow(ui64 hash, TStringBuf key, i64 a, TStringBuf b)
    {
        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedStringValue(b, BId_));
        builder.AddValue(MakeUnversionedUint64Value(hash, HashId_));
        builder.AddValue(MakeUnversionedInt64Value(a, AId_));
        builder.AddValue(MakeUnversionedStringValue(key, KeyId_));
        SourceRows_.push_back(TSourceRow{
            .Key = MakeKey(hash, key),
            .Row = builder.FinishRow(),
        });
    }

    TIntrusivePtr<TStaticTableKeyVisitorJoiner> MakeCursorJoiner()
    {
        using ::testing::_;
        using ::testing::Invoke;

        EXPECT_CALL(*Mock_, GetNode(_, _))
            .WillRepeatedly(Invoke([this] (const NYPath::TYPath& path, const NApi::TGetNodeOptions&) {
                if (path.EndsWith("/@dynamic")) {
                    return MakeFuture(NYson::ConvertToYsonString(false));
                }
                if (path.EndsWith("/@schema")) {
                    return MakeFuture(NYson::ConvertToYsonString(TableSchema_));
                }
                return MakeFuture<NYson::TYsonString>(TError("Unexpected GetNode of %v", path));
            }));
        EXPECT_CALL(*Mock_, CreateTableReader(_, _))
            .WillRepeatedly(Invoke([this] (const NYPath::TRichYPath& path, const NApi::TTableReaderOptions&) {
                auto lower = MinKey();
                auto ranges = path.GetNewRanges(
                    TComparator(std::vector<ESortOrder>{ESortOrder::Ascending, ESortOrder::Ascending}));
                if (!ranges.empty() && ranges[0].LowerLimit().KeyBound()) {
                    lower = TKey(TKey::TUnderlying(ranges[0].LowerLimit().KeyBound().Prefix));
                }
                ReaderOpens_.push_back(lower);
                std::vector<TUnversionedOwningRow> rows;
                for (const auto& sourceRow : SourceRows_) {
                    if (!(sourceRow.Key < lower)) {
                        rows.push_back(sourceRow.Row);
                    }
                }
                return MakeFuture<NApi::ITableReaderPtr>(New<TFakeTableReader>(
                    ReaderNameTable_,
                    TableSchema_,
                    std::move(rows),
                    std::exchange(LeadingEmptyBatches_, 0)));
            }));

        auto context = New<TExternalStateJoinerContext>();
        context->KeySchema = KeySchema_;
        context->ClientsCache = New<TFixedClientsCache>(Mock_);
        context->SerializedInvoker = Queue_->GetInvoker();
        context->StatusProfiler = CreateSyncStatusProfiler();
        context->PipelinePath = NYPath::TRichYPath("//pipeline");
        context->PipelinePath.SetCluster("test");
        context->Logger = NLogging::TLogger("Test");

        auto spec = New<TExternalStateJoinerSpec>();
        spec->ExternalStateJoinerClassName = "NYT::NFlow::TStaticTableKeyVisitorJoiner";
        spec->JoinOn = New<TStateJoinSpec>();
        spec->Parameters = BuildYsonNodeFluently()
            .BeginMap()
            .Item("path")
            .Value("//table")
            .EndMap()
            ->AsMap();
        context->ExternalStateJoinerSpec = std::move(spec);

        auto dynamicContext = New<TDynamicExternalStateJoinerContext>();
        dynamicContext->DynamicExternalStateJoinerSpec = New<TDynamicExternalStateJoinerSpec>();
        return New<TStaticTableKeyVisitorJoiner>(std::move(context), std::move(dynamicContext));
    }

    static IExternalStateJoiner::TFilter MakeFilter(std::optional<TKey> lower, std::optional<TKey> upper)
    {
        return {.LowerKey = std::move(lower), .UpperKey = std::move(upper)};
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TStaticTableJoinerCursorTest, ReusesReaderAcrossForwardLists)
{
    for (ui64 i = 1; i <= 10; ++i) {
        AddSourceRow(i, Format("k%v", i), static_cast<i64>(i), "p");
    }
    auto joiner = MakeCursorJoiner();

    auto first = WaitFor(joiner->List(MakeFilter({}, MakeKey(3u, "")), 100, std::nullopt))
        .ValueOrThrow();
    EXPECT_EQ(std::ssize(first.Keys), 2);

    auto second = WaitFor(joiner->List(MakeFilter(MakeKey(3u, ""), MakeKey(6u, "")), 100, std::nullopt))
        .ValueOrThrow();
    EXPECT_EQ(std::ssize(second.Keys), 3);

    EXPECT_EQ(std::ssize(ReaderOpens_), 1);

    WaitFor(joiner->PreloadKeyStates({MakeKey(ui64(5), "k5")})).ThrowOnError();
    EXPECT_FALSE(joiner->GetState(MakeKey(ui64(5), "k5"))->IsEmpty());
}

TEST_F(TStaticTableJoinerCursorTest, CapHitReReadsLastKeyViaReopen)
{
    for (ui64 i = 1; i <= 5; ++i) {
        AddSourceRow(i, Format("k%v", i), static_cast<i64>(i), "p");
    }
    auto joiner = MakeCursorJoiner();

    auto first = WaitFor(joiner->List(MakeFilter({}, {}), 2, std::nullopt))
        .ValueOrThrow();
    EXPECT_EQ(std::ssize(first.Keys), 2);

    // The visitor re-reads from the last returned key: the cap-hit left it re-readable only
    // through a reopen.
    auto second = WaitFor(joiner->List(MakeFilter(first.Keys.back(), {}), 100, std::nullopt))
        .ValueOrThrow();
    EXPECT_EQ(std::ssize(second.Keys), 4);
    EXPECT_EQ(std::ssize(ReaderOpens_), 2);

    WaitFor(joiner->PreloadKeyStates({MakeKey(ui64(2), "k2"), MakeKey(ui64(5), "k5")})).ThrowOnError();
    EXPECT_FALSE(joiner->GetState(MakeKey(ui64(2), "k2"))->IsEmpty());
    EXPECT_FALSE(joiner->GetState(MakeKey(ui64(5), "k5"))->IsEmpty());
}

TEST_F(TStaticTableJoinerCursorTest, BackwardJumpReopensForwardServesFromOvershoot)
{
    for (ui64 i = 1; i <= 10; ++i) {
        AddSourceRow(i, Format("k%v", i), static_cast<i64>(i), "p");
    }
    auto joiner = MakeCursorJoiner();

    WaitFor(joiner->List(MakeFilter(MakeKey(5u, ""), MakeKey(8u, "")), 100, std::nullopt))
        .ValueOrThrow();
    EXPECT_EQ(std::ssize(ReaderOpens_), 1);

    auto backward = WaitFor(joiner->List(MakeFilter({}, MakeKey(3u, "")), 100, std::nullopt))
        .ValueOrThrow();
    EXPECT_EQ(std::ssize(backward.Keys), 2);
    EXPECT_EQ(std::ssize(ReaderOpens_), 2);

    // The reopened reader has read ahead past the second range: the next forward List is
    // served from the overshoot without another open.
    auto forward = WaitFor(joiner->List(MakeFilter(MakeKey(3u, ""), MakeKey(5u, "")), 100, std::nullopt))
        .ValueOrThrow();
    EXPECT_EQ(std::ssize(forward.Keys), 2);
    EXPECT_EQ(std::ssize(ReaderOpens_), 2);
}

TEST_F(TStaticTableJoinerCursorTest, EmptyBatchIsRetriedViaReadyEvent)
{
    AddSourceRow(1, "k1", 1, "p");
    AddSourceRow(2, "k2", 2, "p");
    LeadingEmptyBatches_ = 2;
    auto joiner = MakeCursorJoiner();

    auto result = WaitFor(joiner->List(MakeFilter({}, {}), 100, std::nullopt))
        .ValueOrThrow();
    EXPECT_EQ(std::ssize(result.Keys), 2);
    EXPECT_EQ(std::ssize(ReaderOpens_), 1);
}

TEST_F(TStaticTableJoinerCursorTest, ExhaustionCoversRangeAndServesBeyondWithoutReopen)
{
    for (ui64 i = 1; i <= 3; ++i) {
        AddSourceRow(i, Format("k%v", i), static_cast<i64>(i), "p");
    }
    auto joiner = MakeCursorJoiner();

    auto first = WaitFor(joiner->List(MakeFilter({}, MakeKey(10u, "")), 100, std::nullopt))
        .ValueOrThrow();
    EXPECT_EQ(std::ssize(first.Keys), 3);

    // The reader is exhausted: a further range is served empty without a reopen, and its keys
    // classify as absent rather than unreadable.
    auto second = WaitFor(joiner->List(MakeFilter(MakeKey(10u, ""), {}), 100, std::nullopt))
        .ValueOrThrow();
    EXPECT_TRUE(second.Keys.empty());
    EXPECT_EQ(std::ssize(ReaderOpens_), 1);

    WaitFor(joiner->PreloadKeyStates({MakeKey(ui64(7), "k7"), MakeKey(ui64(11), "k11")})).ThrowOnError();
    EXPECT_NE(joiner->GetState(MakeKey(ui64(7), "k7")), nullptr);
    EXPECT_TRUE(joiner->GetState(MakeKey(ui64(7), "k7"))->IsEmpty());
    EXPECT_NE(joiner->GetState(MakeKey(ui64(11), "k11")), nullptr);
    EXPECT_TRUE(joiner->GetState(MakeKey(ui64(11), "k11"))->IsEmpty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
