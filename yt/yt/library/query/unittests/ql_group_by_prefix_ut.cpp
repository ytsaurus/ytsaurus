#include <yt/yt/library/query/unittests/evaluate/test_evaluate.h>

#include <yt/yt/library/query/base/query_helpers.h>
#include <yt/yt/library/query/base/join_profiler.h>

#include <yt/yt/library/query/engine_api/coordinator.h>
#include <yt/yt/library/query/engine_api/query_engine_config.h>

#include <yt/yt/library/query/proto/query.pb.h>

#include <yt/yt/library/web_assembly/engine/builtins.h>

#include <yt/yt/client/table_client/pipe.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/unversioned_writer.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NQueryClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TPrefixReaderMock
    : public ISchemafulUnversionedReader
{
public:
    MOCK_METHOD(IUnversionedRowBatchPtr, Read, (const TRowBatchReadOptions& options), (override));
    MOCK_METHOD(TFuture<void>, GetReadyEvent, (), (const, override));
    MOCK_METHOD(bool, IsFetchingCompleted, (), (const, override));
    MOCK_METHOD(std::vector<NChunkClient::TChunkId>, GetFailedChunkIds, (), (const, override));
    MOCK_METHOD(NChunkClient::NProto::TDataStatistics, GetDataStatistics, (), (const, override));
    MOCK_METHOD(NChunkClient::TCodecStatistics, GetDecompressionStatistics, (), (const, override));
};

////////////////////////////////////////////////////////////////////////////////

class TGroupByPrefixTest
    : public TQueryEvaluateTest
{
protected:
    TDataSource MakeSource(std::initializer_list<std::pair<TStringBuf, TStringBuf>> bounds)
    {
        auto rowBuffer = New<TRowBuffer>();
        std::vector<TRowRange> ranges;
        for (const auto& [lower, upper] : bounds) {
            ranges.push_back({rowBuffer->CaptureRow(YsonToKey(lower)), rowBuffer->CaptureRow(YsonToKey(upper))});
        }
        return {.Ranges = MakeSharedRange(std::move(ranges), std::move(rowBuffer))};
    }

    std::pair<TDataSource, TConstQueryPtr> Infer(
        const TConstQueryPtr& query,
        ui64 rangeExpansionLimit = 1000)
    {
        return InferRanges(
            ColumnEvaluatorCache_,
            query,
            TDataSource{.Ranges = MakeSharedRange(TRowRanges())},
            TQueryOptions{
                .RangeExpansionLimit = rangeExpansionLimit,
                .NewRangeInference = true,
            },
            New<TRowBuffer>(),
            GetDefaultMemoryChunkProvider(),
            NLogging::TLogger("Test"));
    }

    std::pair<TSharedRange<TRow>, TQueryStatistics> RunPlan(
        const TConstQueryPtr& query,
        const std::vector<TSource>& tablets,
        EExecutionBackend backend,
        bool optimize)
    {
        auto [inferredSource, inferredQuery] = Infer(query);
        auto [frontQuery, bottomQuery] = GetDistributedQueryPattern(inferredQuery);
        std::vector<TDataSource> dataSources;
        for (int index = 0; index < std::ssize(tablets); ++index) {
            auto source = inferredSource;
            source.ObjectId = TGuid(index + 1, 0, 0, 0);
            dataSources.push_back(std::move(source));
        }

        if (optimize) {
            auto plan = TrySplitGroupByLimitedScanIntoUnionScans(bottomQuery, dataSources);
            YT_VERIFY(plan);
            bottomQuery = std::move(plan->Query);
            dataSources = std::move(plan->DataSources);
        }

        auto [writer, asyncRows] = NApi::CreateSchemafulRowsetWriter(frontQuery->GetTableSchema());
        int index = 0;
        auto statistics = CoordinateAndExecute(
            inferredQuery->ScanOrder,
            /*prefetch*/ false,
            std::ssize(dataSources),
            inferredQuery->Offset,
            inferredQuery->Limit.value_or(std::numeric_limits<i64>::max()),
            /*useAdaptiveOrderedSchemafulReader*/ true,
            [&] () -> TEvaluateResult {
                if (index == std::ssize(dataSources)) {
                    return {};
                }
                const auto& source = dataSources[index++];
                TSource rows;
                for (int tabletIndex = 0; tabletIndex < std::ssize(tablets); ++tabletIndex) {
                    if (source.ObjectId != TGuid(tabletIndex + 1, 0, 0, 0)) {
                        continue;
                    }
                    for (const auto& row : tablets[tabletIndex]) {
                        auto key = YsonToSchemafulRow(row, *query->Schema.Original, /*treatMissingAsNull*/ true);
                        for (const auto& [lower, upper] : source.Ranges) {
                            if (key >= lower && key < upper) {
                                rows.push_back(row);
                                break;
                            }
                        }
                    }
                }
                auto [pipe, leafStatistics] = RunOnNodeThread(bottomQuery, rows, backend);
                return {
                    .Reader = pipe->GetReader(),
                    .Statistics = MakeFuture(std::move(leafStatistics)),
                    .ResponseFeatureFlags = MakeFuture(MostFreshFeatureFlags()),
                };
            },
            [&] (const ISchemafulUnversionedReaderPtr& reader, TFuture<TFeatureFlags> featureFlags) {
                return Evaluator_->Run(
                    frontQuery,
                    reader,
                    writer,
                    TJoinProfilerRegistry({}, {}, nullptr, {}),
                    FunctionProfilers_,
                    AggregateProfilers_,
                    NWebAssembly::GetBuiltinSdk(),
                    GetDefaultMemoryChunkProvider(),
                    TQueryOptions{{.ExecutionBackend = backend, .AllowUnorderedGroupByWithLimit = true}},
                    MostFreshFeatureFlags(),
                    featureFlags);
            },
            /*subplanHolders*/ nullptr,
            /*maxConcurrentSubqueries*/ 2);

        auto rows = NConcurrency::WaitFor(asyncRows)
            .ValueOrThrow()->GetRows();
        return {rows, std::move(statistics)};
    }
};

TEST_F(TGroupByPrefixTest, OptimizationIsDisabledByDefault)
{
    auto config = New<TQueryEngineDynamicConfig>();
    EXPECT_FALSE(config->EnableGroupBySplitLimitedScanIntoUnionScansOptimization.has_value());
    EXPECT_FALSE(config->EnableGroupBySplitLimitedScanIntoUnionScansOptimization.value_or(false));
    config = NYTree::ConvertTo<TQueryEngineDynamicConfigPtr>(NYson::TYsonStringBuf(
        "{enable_group_by_split_limited_scan_into_union_scans_optimization=%true;}"));
    EXPECT_TRUE(config->EnableGroupBySplitLimitedScanIntoUnionScansOptimization.value_or(false));
    config = NYTree::ConvertTo<TQueryEngineDynamicConfigPtr>(NYson::TYsonStringBuf(
        "{enable_group_by_split_limited_scan_into_union_scans_optimization=%false;}"));
    EXPECT_FALSE(config->EnableGroupBySplitLimitedScanIntoUnionScansOptimization.value_or(false));
}

TEST_F(TGroupByPrefixTest, UnionScanConcurrencyConfig)
{
    auto config = New<TQueryEngineDynamicConfig>();
    EXPECT_EQ(config->GroupByUnionScanMaxConcurrentSubqueries, 16);
    EXPECT_FALSE(NYTree::ConvertToNode(config)->AsMap()->FindChild("group_by_union_scan_max_concurrent_subqueries"));

    for (i64 concurrency : {i64{1}, i64{512}, i64{std::numeric_limits<int>::max()} + 1}) {
        config = NYTree::ConvertTo<TQueryEngineDynamicConfigPtr>(NYson::TYsonStringBuf(Format(
            "{group_by_union_scan_max_concurrent_subqueries=%v;}", concurrency)));
        EXPECT_EQ(config->GroupByUnionScanMaxConcurrentSubqueries, concurrency);
        EXPECT_EQ(
            NYTree::ConvertToNode(config)->AsMap()->GetChildOrThrow("group_by_union_scan_max_concurrent_subqueries")->GetValue<i64>(),
            concurrency);
    }

    for (i64 concurrency : {0, -1}) {
        EXPECT_THROW_WITH_SUBSTRING(
            NYTree::ConvertTo<TQueryEngineDynamicConfigPtr>(NYson::TYsonStringBuf(Format(
                "{group_by_union_scan_max_concurrent_subqueries=%v;}", concurrency))),
            "group_by_union_scan_max_concurrent_subqueries");
    }
}

TEST_F(TGroupByPrefixTest, SparseInRangesSurviveSerialization)
{
    auto split = MakeSplit({
        {"prefix", EValueType::Int64, ESortOrder::Ascending},
        {"suffix", EValueType::Int64, ESortOrder::Ascending},
    });
    auto query = Prepare(
        "prefix, first(suffix) as value from [//t] "
        "where prefix in (10, 100, 1000, 100) group by prefix limit 500",
        {{"//t", split}},
        {});

    NProto::TQuery protoQuery;
    auto serializedQuery = New<TQuery>(*query);
    serializedQuery->Prefetching = true;
    ToProto(&protoQuery, serializedQuery);
    TConstQueryPtr restoredQuery;
    FromProto(&restoredQuery, protoQuery);
    EXPECT_EQ(restoredQuery->Limit, 500);
    EXPECT_FALSE(restoredQuery->Prefetching);
    ASSERT_TRUE(restoredQuery->WhereClause->As<TInExpression>());
    EXPECT_EQ(InferName(query->WhereClause), InferName(restoredQuery->WhereClause));

    auto [dataSource, inferredQuery] = Infer(query);
    ASSERT_EQ(dataSource.Ranges.Size(), 3u);

    EXPECT_TRUE(!inferredQuery->WhereClause || IsTrue(inferredQuery->WhereClause));

    NProto::TDataSource protoDataSource;
    ToProto(&protoDataSource, dataSource, /*schema*/ {});
    TDataSource restored;
    FromProto(&restored, protoDataSource, GetDefaultMemoryChunkProvider());
    ASSERT_EQ(restored.Ranges.Size(), 3u);

    const std::vector<int> prefixes = {10, 100, 1000};
    for (int index = 0; index < std::ssize(prefixes); ++index) {
        EXPECT_EQ(restored.Ranges[index].first, YsonToKey(Format("%v", prefixes[index])));
        EXPECT_EQ(restored.Ranges[index].second, YsonToKey(Format("%v;" _MAX_, prefixes[index])));
    }

    auto [frontQuery, bottomQuery] = GetDistributedQueryPattern(inferredQuery);
    EXPECT_EQ(frontQuery->Limit, 500);
    EXPECT_EQ(bottomQuery->Limit, 500);
    EXPECT_FALSE(bottomQuery->IsFinal);
    auto plan = TrySplitGroupByLimitedScanIntoUnionScans(bottomQuery, TRange(&restored, 1));
    ASSERT_TRUE(plan);
    EXPECT_EQ(plan->DataSources.size(), 3u);
}

TEST_F(TGroupByPrefixTest, RangeExpansionLimitCanLoseSingleGroupBounds)
{
    auto split = MakeSplit({
        {"prefix", EValueType::Int64, ESortOrder::Ascending},
        {"suffix", EValueType::Int64, ESortOrder::Ascending},
    });
    auto query = Prepare(
        "prefix, first(suffix) as value from [//t] "
        "where prefix in (10, 100, 1000) group by prefix limit 500",
        {{"//t", split}},
        {});

    auto [dataSource, inferredQuery] = Infer(query, /*rangeExpansionLimit*/ 1);
    ASSERT_EQ(dataSource.Ranges.Size(), 1u);
    EXPECT_LE(dataSource.Ranges[0].first, YsonToKey("10"));
    EXPECT_GT(dataSource.Ranges[0].second, YsonToKey("1000"));
    ASSERT_TRUE(inferredQuery->WhereClause);
    EXPECT_FALSE(IsTrue(inferredQuery->WhereClause));
    auto [frontQuery, bottomQuery] = GetDistributedQueryPattern(inferredQuery);
    EXPECT_FALSE(TrySplitGroupByLimitedScanIntoUnionScans(bottomQuery, TRange(&dataSource, 1)));
}

TEST_F(TGroupByPrefixTest, LegacyReadHintsDoNotEnableOptimization)
{
    auto split = MakeSplit({
        {"prefix", EValueType::Int64, ESortOrder::Ascending},
        {"suffix", EValueType::Int64, ESortOrder::Ascending},
    });
    auto query = Prepare(
        "first(suffix) from [//t] group by prefix limit 500",
        {{"//t", split}},
        {});
    auto source = MakeSource({{"10", "10;" _MAX_}});
    for (auto scanOrder : {EScanOrder::Ordered, EScanOrder::Unordered}) {
        auto unlimitedQuery = New<TQuery>(*query);
        unlimitedQuery->Limit.reset();
        unlimitedQuery->ScanOrder = scanOrder;
        NProto::TQuery serialized;
        ToProto(&serialized, unlimitedQuery);
        TConstQueryPtr restoredQuery;
        FromProto(&restoredQuery, serialized);
        EXPECT_FALSE(restoredQuery->Limit);
        EXPECT_FALSE(TrySplitGroupByLimitedScanIntoUnionScans(restoredQuery, TRange(&source, 1)));
    }
}

TEST_F(TGroupByPrefixTest, ComputedKeyMayBeOmittedFromGrouping)
{
    auto split = MakeSplit({
        TColumnSchema("bucket", EValueType::Int64, ESortOrder::Ascending)
            .SetExpression("prefix % 2"),
        {"prefix", EValueType::Int64, ESortOrder::Ascending},
        {"suffix", EValueType::Int64, ESortOrder::Ascending},
    });

    for (bool omitComputedKey : {false, true}) {
        SCOPED_TRACE(omitComputedKey);
        auto query = Prepare(
            Format(
                "prefix, first(suffix) as value from [//t] "
                "where prefix in (10, 11, 1000) group by %v limit 500",
                omitComputedKey ? "prefix" : "bucket, prefix"),
            {{"//t", split}},
            {});

        ASSERT_TRUE(query->GroupClause);
        // This is a count of grouping items, not the physical prefix width (two).
        EXPECT_EQ(query->GroupClause->CommonPrefixWithPrimaryKey, omitComputedKey ? 1u : 2u);

        NProto::TQuery protoQuery;
        ToProto(&protoQuery, query);
        TConstQueryPtr restoredQuery;
        FromProto(&restoredQuery, protoQuery);
        EXPECT_EQ(restoredQuery->Schema.Original->GetKeyColumnCount(), 3);
        EXPECT_EQ(restoredQuery->Schema.Original->Columns()[0].Expression(), "prefix % 2");

        auto [dataSource, inferredQuery] = Infer(restoredQuery);
        ASSERT_EQ(dataSource.Ranges.Size(), 3u);
        auto [frontQuery, bottomQuery] = GetDistributedQueryPattern(inferredQuery);
        auto plan = TrySplitGroupByLimitedScanIntoUnionScans(bottomQuery, TRange(&dataSource, 1));
        ASSERT_TRUE(plan);
        EXPECT_EQ(plan->DataSources.size(), 3u);
        const std::vector<std::pair<int, int>> keys = {{0, 10}, {0, 1000}, {1, 11}};
        for (int index = 0; index < std::ssize(keys); ++index) {
            auto [bucket, prefix] = keys[index];
            EXPECT_EQ(dataSource.Ranges[index].first, YsonToKey(Format("%v;%v", bucket, prefix)));
            EXPECT_EQ(dataSource.Ranges[index].second, YsonToKey(Format("%v;%v;" _MAX_, bucket, prefix)));
        }
    }
}

TEST_F(TGroupByPrefixTest, SingleGroupLimitReducesReadWeight)
{
    auto split = MakeSplit({
        {"prefix", EValueType::Int64, ESortOrder::Ascending},
        {"suffix", EValueType::Int64, ESortOrder::Ascending},
    });
    TSource source;
    for (int suffix = 0; suffix < 1000; ++suffix) {
        source.push_back(Format("prefix=10;suffix=%v", suffix));
    }
    auto expected = YsonToRows(
        {"prefix=10;value=0"},
        MakeSplit({
            {"prefix", EValueType::Int64},
            {"value", EValueType::Int64},
        }));

    auto evaluate = [&] (int limit) {
        return EvaluateWithQueryStatistics(
            Format("prefix, first(suffix) as value from [//t] group by prefix limit %v", limit),
            split,
            source,
            ResultMatcher(expected)).second;
    };

    auto original = evaluate(500);
    auto reduced = evaluate(1);
    EXPECT_EQ(original.RowsRead.GetTotal(), std::ssize(source));
    EXPECT_GT(reduced.DataWeightRead.GetTotal(), 0);
    EXPECT_EQ(reduced.RowsRead.GetTotal(), 3);
    EXPECT_EQ(original.DataWeightRead.GetTotal() * 3, reduced.DataWeightRead.GetTotal() * std::ssize(source));
}

TEST_F(TGroupByPrefixTest, FirstNullStateAndLimitInteraction)
{
    auto split = MakeSplit({
        {"prefix", EValueType::Int64, ESortOrder::Ascending},
        {"suffix", EValueType::Int64, ESortOrder::Ascending},
        {"value", EValueType::Int64},
    });
    const TSource source = {
        "prefix=10;suffix=0;value=#",
        "prefix=10;suffix=1;value=#",
        "prefix=10;suffix=2;value=7",
        "prefix=10;suffix=3;value=8",
    };
    auto resultSplit = MakeSplit({{"value", EValueType::Int64}});

    Evaluate(
        "first(value) as value from [//t] group by prefix limit 500",
        split,
        source,
        ResultMatcher(YsonToRows({"value=7"}, resultSplit)));

    Evaluate(
        "first(value) as value from [//t] group by prefix limit 1",
        split,
        source,
        ResultMatcher(YsonToRows({"value=#"}, resultSplit)));
}

TEST_F(TGroupByPrefixTest, TabletBoundaryGroupsAreMerged)
{
    auto split = MakeSplit({
        {"prefix", EValueType::Int64, ESortOrder::Ascending},
        {"suffix", EValueType::Int64, ESortOrder::Ascending},
    });
    auto expected = YsonToRows(
        {"prefix=10;value=0", "prefix=100;value=0"},
        MakeSplit({
            {"prefix", EValueType::Int64},
            {"value", EValueType::Int64},
        }));

    for (auto backend : {EExecutionBackend::Native, EExecutionBackend::WebAssembly}) {
        if (backend == EExecutionBackend::WebAssembly && !EnableWebAssemblyInUnitTests()) {
            continue;
        }
        EvaluateFullCoordinatedGroupByImpl(
            "prefix, first(suffix) as value from [//t] group by prefix limit 500",
            split,
            {
                {{"prefix=10;suffix=0", "prefix=10;suffix=1"}},
                {{"prefix=10;suffix=2"}, {"prefix=10;suffix=3", "prefix=100;suffix=0"}},
            },
            ResultMatcher(expected),
            backend);
    }
}

TEST_F(TGroupByPrefixTest, PlanKeepsEachGroupAndTabletSeparate)
{
    auto split = MakeSplit({
        {"prefix", EValueType::Int64, ESortOrder::Ascending},
        {"suffix", EValueType::Int64, ESortOrder::Ascending},
    });
    auto query = Prepare(
        "prefix, first(suffix) as value from [//t] group by prefix order by value desc limit 500",
        {{"//t", split}},
        {});
    auto [frontQuery, bottomQuery] = GetDistributedQueryPattern(query);
    auto sources = std::vector<TDataSource>{
        MakeSource({{"10;0", "10;2"}, {"10;4", "10;8"}, {"100", "100;" _MAX_}}),
        MakeSource({{"10;8", "10;" _MAX_}}),
    };
    sources[0].ObjectId = TGuid(1, 0, 0, 0);
    sources[1].ObjectId = TGuid(2, 0, 0, 0);

    auto plan = TrySplitGroupByLimitedScanIntoUnionScans(bottomQuery, sources);
    ASSERT_TRUE(plan);
    ASSERT_EQ(plan->DataSources.size(), 3u);
    EXPECT_EQ(plan->DataSources[0].Ranges.Size(), 2u);
    EXPECT_EQ(plan->DataSources[1].Ranges.Size(), 1u);
    EXPECT_EQ(plan->DataSources[2].Ranges.Size(), 1u);
    EXPECT_EQ(plan->DataSources[0].ObjectId, sources[0].ObjectId);
    EXPECT_EQ(plan->DataSources[2].ObjectId, sources[1].ObjectId);
    EXPECT_EQ(plan->Query->Limit, 1);
    EXPECT_EQ(plan->Query->Offset, 0);
    EXPECT_EQ(plan->Query->ScanOrder, EScanOrder::Ordered);
    EXPECT_FALSE(plan->Query->OrderClause);
    EXPECT_FALSE(plan->Query->IsFinal);
    EXPECT_TRUE(frontQuery->OrderClause);
    EXPECT_EQ(frontQuery->Limit, 500);
    EXPECT_TRUE(bottomQuery->OrderClause);
    EXPECT_EQ(bottomQuery->Limit, 500);
}

TEST_F(TGroupByPrefixTest, PlanRequiresFixedPhysicalPrefix)
{
    auto split = MakeSplit({
        TColumnSchema("bucket", EValueType::Int64, ESortOrder::Ascending).SetExpression("prefix % 2"),
        {"prefix", EValueType::Int64, ESortOrder::Ascending},
        {"suffix", EValueType::Int64, ESortOrder::Ascending},
    });
    for (const auto& grouping : {"t.prefix", "t.bucket, t.prefix"}) {
        auto query = Prepare(
            Format("first(t.suffix) from [//t] as t group by %v limit 500", grouping),
            {{"//t", split}},
            {});
        auto [frontQuery, bottomQuery] = GetDistributedQueryPattern(query);
        for (const auto& source : {
            MakeSource({{"0;10", "0;10;" _MAX_}}),
            MakeSource({{"0;10", "0;10;20"}}),
            MakeSource({{"0;10;5", "0;10;20"}}),
            MakeSource({{"#;#", "#;#;" _MAX_}}),
        }) {
            EXPECT_TRUE(TrySplitGroupByLimitedScanIntoUnionScans(bottomQuery, TRange(&source, 1)));
        }
        for (const auto& source : {
            MakeSource({{"0", "0;" _MAX_}}),
            MakeSource({{"0;10;5", "1"}}),
            MakeSource({{"0;10", "0;100;" _MAX_}}),
            MakeSource({{"0;10", "1;10;" _MAX_}}),
            MakeSource({{"0;10", "0;10;" _MAX_}, {"0;100", "0;1000;" _MAX_}}),
            MakeSource({{"", _MAX_}}),
        }) {
            EXPECT_FALSE(TrySplitGroupByLimitedScanIntoUnionScans(bottomQuery, TRange(&source, 1)));
        }
    }
}

TEST_F(TGroupByPrefixTest, GroupingMayOmitOrdinaryKeyColumns)
{
    auto split = MakeSplit({
        {"k0", EValueType::Int64, ESortOrder::Ascending},
        {"k1", EValueType::Int64, ESortOrder::Ascending},
        {"k2", EValueType::Int64, ESortOrder::Ascending},
        {"suffix", EValueType::Int64, ESortOrder::Ascending},
        {"value", EValueType::Int64},
    });
    const std::vector<TSource> tablets = {
        {
            "k0=1;k1=10;k2=7;suffix=0;value=70",
            "k0=1;k1=10;k2=7;suffix=1;value=70",
            "k0=1;k1=20;k2=7;suffix=0;value=70",
            "k0=1;k1=20;k2=7;suffix=1;value=70",
            "k0=2;k1=30;k2=8;suffix=0;value=80",
            "k0=2;k1=30;k2=8;suffix=1;value=80",
        },
    };
    for (const auto* groupKeys : {"k0, k2", "k2"}) {
        auto query = Prepare(
            Format(
                "k2, first(value) from [//t] "
                "where (k0, k1, k2) in ((1, 10, 7), (1, 20, 7), (2, 30, 8)) "
                "group by %v order by k2 limit 500",
                groupKeys),
            {{"//t", split}},
            {});
        auto [expected, baselineStatistics] = RunPlan(query, tablets, EExecutionBackend::Native, /*optimize*/ false);
        auto [actual, optimizedStatistics] = RunPlan(query, tablets, EExecutionBackend::Native, /*optimize*/ true);
        EXPECT_EQ(actual.Size(), 2u);
        EXPECT_EQ(std::vector<TRow>(actual.begin(), actual.end()), std::vector<TRow>(expected.begin(), expected.end()));
    }
}

TEST_F(TGroupByPrefixTest, IneligibleQueriesKeepOriginalPlan)
{
    auto split = MakeSplit({
        {"prefix", EValueType::Int64, ESortOrder::Ascending},
        {"suffix", EValueType::Int64, ESortOrder::Ascending},
        {"value", EValueType::Int64},
    });
    auto source = MakeSource({{"10", "10;" _MAX_}});
    for (const auto& text : {
        "prefix from [//t] limit 500",
        "sum(value) from [//t] group by prefix limit 500",
        "first(value) from [//t] group by prefix order by sum(value) limit 500",
        "first(value) from [//t] group by prefix having first(value) > 0",
        "first(value) from [//t] group by prefix with totals",
        "first(value) from [//t] group by prefix + 1 limit 500",
        "first(value) from [//t] group by value limit 500",
        "first(value) from [//t] group by prefix, suffix limit 500",
        "first(value) from [//t] group by prefix",
        "first(value) from [//t] group by prefix limit 0",
        "first(t.value) from [//t] as t join [//u] as u on t.prefix = u.prefix group by t.prefix limit 500",
    }) {
        SCOPED_TRACE(text);
        auto query = Prepare(text, {{"//t", split}, {"//u", split}}, {});
        if (query->HavingClause || (query->GroupClause && query->GroupClause->TotalsMode != ETotalsMode::None)) {
            query->Limit = 500;
        }
        auto [frontQuery, bottomQuery] = GetDistributedQueryPattern(query);
        EXPECT_FALSE(TrySplitGroupByLimitedScanIntoUnionScans(bottomQuery, TRange(&source, 1)));
    }
}

TEST_F(TGroupByPrefixTest, OptimizedExecutionMergesGroupsAndReducesReadWeight)
{
    for (bool computed : {false, true}) {
        auto columns = std::vector<TColumnSchema>{
            {"prefix", EValueType::Int64, ESortOrder::Ascending},
            {"suffix", EValueType::Int64, ESortOrder::Ascending},
            {"value", EValueType::Int64},
        };
        if (computed) {
            columns.insert(
                columns.begin(),
                TColumnSchema("bucket", EValueType::Int64, ESortOrder::Ascending).SetExpression("prefix % 2"));
        }
        auto split = MakeSplit(columns);
        std::vector<TSource> tablets(2);
        for (int prefix : {10, 100, 1000}) {
            for (int suffix = 0; suffix < 2048; ++suffix) {
                tablets[prefix == 10 && suffix < 1024 ? 0 : 1].push_back(Format(
                    "%vprefix=%v;suffix=%v;value=%v",
                    computed ? "bucket=0;" : "",
                    prefix,
                    suffix,
                    prefix * 10));
            }
        }
        for (const auto& ending : {
            "limit 500",
            "order by value desc limit 500",
            "order by prefix desc offset 1 limit 1",
            "limit 2",
        }) {
            auto query = Prepare(
                Format(
                    "prefix, first(value) as value from [//t] "
                    "where prefix in (10, 100, 1000, 9999) and suffix >= 2 "
                    "group by prefix %v",
                    ending),
                {{"//t", split}},
                {});
            for (auto backend : {EExecutionBackend::Native, EExecutionBackend::WebAssembly}) {
                if (backend == EExecutionBackend::WebAssembly && !EnableWebAssemblyInUnitTests()) {
                    continue;
                }
                SCOPED_TRACE(Format("Computed: %v, Ending: %v, Backend: %v", computed, ending, backend));
                auto [expected, baseline] = RunPlan(query, tablets, backend, /*optimize*/ false);
                auto [actual, optimized] = RunPlan(query, tablets, backend, /*optimize*/ true);
                EXPECT_EQ(actual.Size(), std::min<i64>(3 - query->Offset, *query->Limit));
                EXPECT_EQ(std::vector<TRow>(actual.begin(), actual.end()), std::vector<TRow>(expected.begin(), expected.end()));
                auto readWeight = [] (const TQueryStatistics& statistics) {
                    i64 result = 0;
                    for (const auto& leaf : statistics.InnerStatistics) {
                        result += leaf.DataWeightRead.GetTotal();
                    }
                    return result;
                };
                EXPECT_GT(readWeight(optimized), 0);
                EXPECT_LT(readWeight(optimized), readWeight(baseline) / 2);
            }
        }
    }
}

TEST_F(TGroupByPrefixTest, OptimizedFirstMayReturnNull)
{
    auto split = MakeSplit({
        {"prefix", EValueType::Int64, ESortOrder::Ascending},
        {"suffix", EValueType::Int64, ESortOrder::Ascending},
        {"value", EValueType::Int64},
    });
    auto query = Prepare(
        "first(value) as value from [//t] where prefix = 10 group by prefix limit 500",
        {{"//t", split}},
        {});
    auto [rows, statistics] = RunPlan(
        query,
        {
            {
                "prefix=10;suffix=0;value=#",
                "prefix=10;suffix=1;value=7",
            },
        },
        EExecutionBackend::Native,
        /*optimize*/ true);
    ASSERT_EQ(rows.Size(), 1u);
    EXPECT_EQ(rows[0][0].Type, EValueType::Null);
}

TEST_F(TGroupByPrefixTest, ExhaustiveSmallInputs)
{
    const std::vector<std::pair<bool, std::string>> groupings = {
        {false, "prefix"},
        {true, "prefix"},
        {true, "bucket, prefix"},
    };
    const std::vector<i64> prefixes = {10, 101, 1000};
    for (const auto& [computed, groupKeys] : groupings) {
        auto columns = std::vector<TColumnSchema>{
            {"prefix", EValueType::Int64, ESortOrder::Ascending},
            {"suffix", EValueType::Int64, ESortOrder::Ascending},
            {"value", EValueType::Int64},
        };
        if (computed) {
            columns.insert(
                columns.begin(),
                TColumnSchema("bucket", EValueType::Int64, ESortOrder::Ascending).SetExpression("prefix % 2"));
        }
        auto split = MakeSplit(columns);
        TSource source;
        for (int index = 0; index < 4; ++index) {
            auto prefix = prefixes[index / 2];
            source.push_back(Format(
                "%vprefix=%v;suffix=%v;value=%v",
                computed ? Format("bucket=%v;", prefix % 2) : "",
                prefix,
                index % 2,
                index % 2 ? ToString(prefix * 10) : "#"));
        }
        for (ui32 inMask = 0; inMask < 8; ++inMask) {
            std::vector<i64> inValues;
            for (int index = 0; index < std::ssize(prefixes); ++index) {
                if (inMask & (1u << index)) {
                    inValues.push_back(prefixes[index]);
                }
            }
            if (!inValues.empty()) {
                inValues.push_back(inValues.front());
            }
            for (const auto* ending : {
                "limit 500",
                "order by prefix limit 1",
                "order by prefix desc offset 1 limit 1",
            }) {
                auto query = Prepare(
                    Format(
                        "prefix, first(value) as value from [//t] where prefix in (%v) group by %v %v",
                        JoinToString(inValues),
                        groupKeys,
                        ending),
                    {{"//t", split}},
                    {});
                for (ui32 rowMask = 0; rowMask < 16; ++rowMask) {
                    std::vector<i64> expected;
                    for (int index = 0; index < 2; ++index) {
                        if ((inMask & (1u << index)) && (rowMask & (3u << (2 * index)))) {
                            expected.push_back(prefixes[index]);
                        }
                    }
                    if (query->OrderClause && query->OrderClause->OrderItems[0].Descending) {
                        std::reverse(expected.begin(), expected.end());
                    }
                    expected.erase(expected.begin(), expected.begin() + std::min<i64>(query->Offset, expected.size()));
                    expected.resize(std::min<i64>(*query->Limit, expected.size()));
                    for (int cut = 0; cut <= std::ssize(source); ++cut) {
                        SCOPED_TRACE(Format(
                            "Computed: %v, GroupKeys: %v, InMask: %v, Ending: %v, RowMask: %v, Cut: %v",
                            computed,
                            groupKeys,
                            inMask,
                            ending,
                            rowMask,
                            cut));
                        std::vector<TSource> tablets(2);
                        for (int index = 0; index < std::ssize(source); ++index) {
                            if (rowMask & (1u << index)) {
                                tablets[index < cut ? 0 : 1].push_back(source[index]);
                            }
                        }
                        auto [rows, statistics] = RunPlan(query, tablets, EExecutionBackend::Native, /*optimize*/ true);
                        auto actual = std::vector<TRow>(rows.begin(), rows.end());
                        if (!query->OrderClause) {
                            std::sort(actual.begin(), actual.end());
                        }
                        ASSERT_EQ(actual.size(), expected.size());
                        for (int index = 0; index < std::ssize(actual); ++index) {
                            ASSERT_EQ(actual[index].GetCount(), 2);
                            EXPECT_EQ(actual[index][0], MakeInt64(expected[index]));
                            bool isNull = actual[index][1].Type == EValueType::Null;
                            int prefixIndex = expected[index] == prefixes[0] ? 0 : 1;
                            EXPECT_TRUE(rowMask & (1u << (2 * prefixIndex + !isNull)));
                            if (!isNull) {
                                EXPECT_EQ(actual[index][1], MakeInt64(expected[index] * 10));
                            }
                        }
                    }
                }
            }
        }
    }
}

TEST_F(TGroupByPrefixTest, LocalSubqueryConcurrencyIsBounded)
{
    auto split = MakeSplit({
        {"prefix", EValueType::Int64, ESortOrder::Ascending},
        {"suffix", EValueType::Int64, ESortOrder::Ascending},
    });
    auto query = Prepare(
        "suffix from [//t] limit 1",
        {{"//t", split}},
        {});
    auto config = New<TQueryEngineDynamicConfig>();
    const std::vector<std::tuple<EScanOrder, int, i64>> cases = {
        {EScanOrder::Ordered, 17, 1},
        {EScanOrder::Ordered, 17, 3},
        {EScanOrder::Ordered, 17, std::numeric_limits<i64>::max()},
        {EScanOrder::Reversed, 17, 3},
        {EScanOrder::Reversed, 17, std::numeric_limits<i64>::max()},
        {EScanOrder::Unordered, 17, 1},
        {EScanOrder::Unordered, 17, 3},
        {EScanOrder::Unordered, 17, std::numeric_limits<i64>::max()},
        {EScanOrder::Ordered, 512, config->GroupByUnionScanMaxConcurrentSubqueries},
        {EScanOrder::Reversed, 512, config->GroupByUnionScanMaxConcurrentSubqueries},
        {EScanOrder::Unordered, 512, config->GroupByUnionScanMaxConcurrentSubqueries},
        {EScanOrder::Unordered, 512, std::numeric_limits<i64>::max()},
    };
    for (const auto& [scanOrder, splitCount, maxConcurrentSubqueries] : cases) {
        SCOPED_TRACE(Format("ScanOrder: %v, SplitCount: %v, MaxConcurrentSubqueries: %v",
            scanOrder, splitCount, maxConcurrentSubqueries));
        for (bool prefetch : {false, true}) {
            if (prefetch && maxConcurrentSubqueries < splitCount) {
                continue;
            }
            for (bool adaptive : {false, true}) {
                int started = 0;
                int active = 0;
                int peak = 0;
                std::vector<i64> result;
                CoordinateAndExecute(
                    scanOrder,
                    prefetch,
                    splitCount,
                    /*offset*/ 0,
                    /*limit*/ 500,
                    adaptive,
                    [&] () -> TEvaluateResult {
                        if (started == splitCount) {
                            return {};
                        }
                        auto [pipe, statistics] = RunOnNodeThread(
                            query,
                            {Format("prefix=0;suffix=%v", started++)},
                            EExecutionBackend::Native);
                        peak = std::max(peak, ++active);
                        auto reader = New<NiceMock<TPrefixReaderMock>>();
                        ON_CALL(*reader, GetReadyEvent()).WillByDefault(Return(OKFuture));
                        EXPECT_CALL(*reader, Read(_)).WillRepeatedly([
                            &,
                            underlying = pipe->GetReader(),
                            finished = false
                        ] (const TRowBatchReadOptions& options) mutable {
                            auto batch = underlying->Read(options);
                            if (!batch && !std::exchange(finished, true)) {
                                --active;
                            }
                            return batch;
                        });
                        return {
                            .Reader = reader,
                            .Statistics = MakeFuture(std::move(statistics)),
                            .ResponseFeatureFlags = MakeFuture(MostFreshFeatureFlags()),
                        };
                    },
                    [&] (const ISchemafulUnversionedReaderPtr& reader, TFuture<TFeatureFlags> /*featureFlags*/) {
                        while (auto batch = reader->Read()) {
                            for (auto row : batch->MaterializeRows()) {
                                result.push_back(row[0].Data.Int64);
                            }
                            NConcurrency::WaitFor(reader->GetReadyEvent())
                                .ThrowOnError();
                        }
                        return TQueryStatistics();
                    },
                    /*subplanHolders*/ nullptr,
                    maxConcurrentSubqueries);
                EXPECT_EQ(started, splitCount);
                EXPECT_EQ(active, 0);
                auto expectedPeak = std::min<i64>(splitCount, maxConcurrentSubqueries);
                EXPECT_LE(peak, expectedPeak);
                if (scanOrder == EScanOrder::Unordered || prefetch ||
                    (adaptive && maxConcurrentSubqueries < splitCount))
                {
                    EXPECT_EQ(peak, expectedPeak);
                }
                ASSERT_EQ(std::ssize(result), splitCount);
                if (scanOrder == EScanOrder::Unordered) {
                    std::sort(result.begin(), result.end());
                }
                for (int index = 0; index < std::ssize(result); ++index) {
                    EXPECT_EQ(result[index], index);
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
