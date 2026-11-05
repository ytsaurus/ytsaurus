#include <yt/yt/library/query/unittests/evaluate/test_evaluate.h>

#include <yt/yt/library/web_assembly/engine/builtins.h>

#include <yt/yt/client/table_client/pipe.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unordered_schemaful_reader.h>
#include <yt/yt/client/table_client/unversioned_reader.h>

#include <iterator>
#include <utility>
#include <vector>

namespace NYT::NQueryClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST_F(TQueryEvaluateTest, UnionAllSelectsSecondColumn)
{
    auto split = MakeSplit({
        {"unused", EValueType::Int64},
        {"value", EValueType::Int64},
    });
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t1"))
        .WillOnce(Return(MakeFuture(split)));
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t2"))
        .WillOnce(Return(MakeFuture(split)));

    auto fragment = ParseAndPreparePlanFragment(
        &PrepareMock_,
        "SELECT value FROM ("
        "SELECT unused, value FROM [//t1] "
        "UNION ALL "
        "SELECT unused, value FROM [//t2])");
    ASSERT_TRUE(fragment->UnionAllQuery);
    ASSERT_EQ(2, std::ssize(fragment->UnionAllQuery->SubqueryFragments));

    const std::vector<TSource> sources = {
        {"unused=1;value=100"},
        {"unused=2;value=200"},
    };
    std::vector<ISchemafulUnversionedReaderPtr> readers;
    for (int index = 0; index < std::ssize(sources); ++index) {
        readers.push_back(RunOnNodeThread(
            fragment->UnionAllQuery->SubqueryFragments[index]->Query,
            sources[index],
            EExecutionBackend::Native)->GetReader());
    }

    auto getNextReader = [readers = std::move(readers), index = 0] () mutable {
        return index < std::ssize(readers)
            ? readers[index++]
            : ISchemafulUnversionedReaderPtr();
    };
    auto pipe = CreateSchemafulPipe(GetDefaultMemoryChunkProvider());
    Evaluator_->Run(
        fragment->Query,
        CreateUnorderedSchemafulReader(std::move(getNextReader), /*concurrency*/ 2),
        pipe->GetWriter(),
        /*joinProfilerRegistry*/ TJoinProfilerRegistry(
            /*executePlan*/ {},
            /*consumeSubqueryStatistics*/ {},
            /*memoryChunkProvider*/ nullptr,
            /*logger*/ {}),
        FunctionProfilers_,
        AggregateProfilers_,
        NWebAssembly::GetBuiltinSdk(),
        GetDefaultMemoryChunkProvider(),
        TQueryOptions{{.ExecutionBackend = EExecutionBackend::Native}},
        MostFreshFeatureFlags(),
        MakeFuture(MostFreshFeatureFlags()));

    auto reader = pipe->GetReader();
    auto buffer = New<TRowBuffer>();
    std::vector<TRow> rows;
    while (auto batch = reader->Read()) {
        ASSERT_FALSE(batch->IsEmpty());
        for (auto row : batch->MaterializeRows()) {
            rows.push_back(buffer->CaptureRow(row));
        }
    }

    ASSERT_EQ(2, std::ssize(rows));

    auto resultSplit = MakeSplit({{"value", EValueType::Int64}});
    OrderedResultMatcher(
        YsonToRows({"value=100", "value=200"}, resultSplit),
        {"value"})(rows, *fragment->Query->GetTableSchema());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
