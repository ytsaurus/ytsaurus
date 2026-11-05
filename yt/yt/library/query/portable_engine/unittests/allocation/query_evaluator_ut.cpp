#include <yt/yt/library/query/engine_api/query_evaluator.h>

#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/yt/string/format.h>
#include <library/cpp/yt/string/string.h>

#include <library/cpp/lfalloc/dbg_info/dbg_info.h>

#include <library/cpp/malloc/api/malloc.h>

namespace NYT::NQueryClient::NPortable::NTest {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

thread_local i64* CurrentAllocationCounter = nullptr;

int RecordAllocation(int /*tag*/, size_t /*size*/, int /*sizeIdx*/)
{
    if (CurrentAllocationCounter) {
        ++*CurrentAllocationCounter;
    }

    return DBG_ALLOC_INVALID_COOKIE;
}

TValue EvaluateQueryWithAllocationCounter(
    const TQueryEvaluationContext& context,
    TRange<TValue> inputValues,
    const TRowBufferPtr& rowBuffer,
    i64* allocationCount)
{
    bool previousSamplingEnabled = NAllocDbg::SetAllocationSamplingEnabled(false);
    bool previousProfileAllThreads = NAllocDbg::SetProfileAllThreads(false);
    bool previousProfileCurrentThread = NAllocDbg::SetProfileCurrentThread(true);
    size_t previousSampleRate = NAllocDbg::SetAllocationSampleRate(/*newVal*/ 1);
    auto* previousAllocationCallback = NAllocDbg::SetAllocationCallback(&RecordAllocation);
    auto* previousCounter = std::exchange(CurrentAllocationCounter, allocationCount);
    auto restoreProfiler = Finally([&] {
        NAllocDbg::SetAllocationSamplingEnabled(false);
        CurrentAllocationCounter = previousCounter;
        NAllocDbg::SetAllocationCallback(previousAllocationCallback);
        NAllocDbg::SetAllocationSampleRate(previousSampleRate);
        NAllocDbg::SetProfileCurrentThread(previousProfileCurrentThread);
        NAllocDbg::SetProfileAllThreads(previousProfileAllThreads);
        NAllocDbg::SetAllocationSamplingEnabled(previousSamplingEnabled);
    });

    NAllocDbg::SetAllocationSamplingEnabled(true);
    return EvaluateQuery(context, inputValues, rowBuffer);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPortableQueryEvaluatorAllocationTest, SmallExpressionDoesNotAllocate)
{
    ASSERT_TRUE(NMalloc::MallocInfo().GetParam("SetAllocationCallback"));

    constexpr int ArgumentCount = 15;
    std::vector<std::string> columnNames;
    std::vector<TColumnSchema> columns;
    std::vector<TValue> inputValues;
    for (int index = 0; index < ArgumentCount; ++index) {
        auto name = Format("u%v", index);
        columnNames.push_back(name);
        columns.push_back(TColumnSchema(name, EValueType::Uint64));
        inputValues.push_back(MakeUnversionedUint64Value(index + 1));
    }

    auto schema = New<TTableSchema>(std::move(columns));
    auto parsedSource = ParseSource(
        Format("farm_hash(%v)", JoinToString(columnNames)),
        EParseMode::Expression);
    auto expression = PrepareExpression(*parsedSource, *schema);
    auto context = CreateQueryEvaluationContext(expression, schema);
    auto rowBuffer = New<TRowBuffer>();

    for (int iteration = 0; iteration < 2; ++iteration) {
        SCOPED_TRACE(iteration);
        i64 allocationCount = 0;
        auto result = EvaluateQueryWithAllocationCounter(*context, inputValues, rowBuffer, &allocationCount);
        auto expected = MakeUnversionedUint64Value(GetFarmFingerprint(TRange<TValue>(inputValues)));

        EXPECT_EQ(expected, result);
        EXPECT_EQ(0, allocationCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient::NPortable::NTest
