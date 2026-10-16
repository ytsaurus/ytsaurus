#include <benchmark/benchmark.h>

#include <yt/yt/flow/library/cpp/misc/compact_unversioned_owning_row.h>

#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

constexpr int VectorSize = 10000;
constexpr TStringBuf StringValue = "abracabadra";

////////////////////////////////////////////////////////////////////////////////

std::vector<TUnversionedOwningRow> BuildOwningRows()
{
    std::vector<TUnversionedOwningRow> rows;
    TUnversionedOwningRowBuilder builder;
    for (int i = 0; i < VectorSize; ++i) {
        bool hasExtraColumn = (i % 2 == 1);
        builder.AddValue(MakeUnversionedNullValue(0));
        builder.AddValue(MakeUnversionedInt64Value(i, 1));
        builder.AddValue(MakeUnversionedStringValue(StringValue, 2));
        builder.AddValue(MakeUnversionedInt64Value(i, 3));
        if (hasExtraColumn) {
            builder.AddValue(MakeUnversionedStringValue(StringValue, 4));
        }
        rows.push_back(builder.FinishRow());
    }
    return rows;
}

std::vector<TCompactUnversionedOwningRow> BuildCompactRows()
{
    std::vector<TCompactUnversionedOwningRow> rows;
    // Use TUnversionedRowBuilder (non-owning) to avoid an intermediate TUnversionedOwningRow allocation.
    // GetRow() returns a view into the builder's internal buffer which stays alive during construction.
    TUnversionedRowBuilder builder;
    for (int i = 0; i < VectorSize; ++i) {
        bool hasExtraColumn = (i % 2 == 1);
        builder.AddValue(MakeUnversionedNullValue(0));
        builder.AddValue(MakeUnversionedInt64Value(i, 1));
        builder.AddValue(MakeUnversionedStringValue(StringValue, 2));
        builder.AddValue(MakeUnversionedInt64Value(i, 3));
        if (hasExtraColumn) {
            builder.AddValue(MakeUnversionedStringValue(StringValue, 4));
        }
        rows.emplace_back(TCompactUnversionedOwningRow(builder.GetRow()));
        builder.Reset();
    }
    return rows;
}

std::vector<TCompactUnversionedOwningRow> BuildCompactRowsWithFunctor()
{
    std::vector<TCompactUnversionedOwningRow> rows;
    for (int i = 0; i < VectorSize; ++i) {
        bool hasExtraColumn = (i % 2 == 1);
        int count = hasExtraColumn ? 5 : 4;
        size_t stringDataSize = StringValue.size() * (hasExtraColumn ? 2 : 1);
        rows.emplace_back(count, stringDataSize, [&] (TMutableUnversionedRow row) {
            row[0] = MakeUnversionedNullValue(0);
            row[1] = MakeUnversionedInt64Value(i, 1);
            row[2] = MakeUnversionedStringValue(StringValue, 2);
            row[3] = MakeUnversionedInt64Value(i, 3);
            if (hasExtraColumn) {
                row[4] = MakeUnversionedStringValue(StringValue, 4);
            }
        });
    }
    return rows;
}

std::vector<TCompactUnversionedOwningRow> BuildCompactRowsWithMake()
{
    std::vector<TCompactUnversionedOwningRow> rows;
    for (int i = 0; i < VectorSize; ++i) {
        bool hasExtraColumn = (i % 2 == 1);
        if (hasExtraColumn) {
            rows.push_back(MakeCompactUnversionedOwningRow(
                MakeUnversionedNullValue(0),
                MakeUnversionedInt64Value(i, 1),
                MakeUnversionedStringValue(StringValue, 2),
                MakeUnversionedInt64Value(i, 3),
                MakeUnversionedStringValue(StringValue, 4)));
        } else {
            rows.push_back(MakeCompactUnversionedOwningRow(
                MakeUnversionedNullValue(0),
                MakeUnversionedInt64Value(i, 1),
                MakeUnversionedStringValue(StringValue, 2),
                MakeUnversionedInt64Value(i, 3)));
        }
    }
    return rows;
}

const std::vector<TUnversionedOwningRow> OwningRows = BuildOwningRows();
const std::vector<TCompactUnversionedOwningRow> CompactRows = BuildCompactRows();
const std::vector<TCompactUnversionedOwningRow> CompactRowsWithFunctor = BuildCompactRowsWithFunctor();
const std::vector<TCompactUnversionedOwningRow> CompactRowsWithMake = BuildCompactRowsWithMake();

////////////////////////////////////////////////////////////////////////////////

void BM_UnversionedOwningRowBuild(benchmark::State& state)
{
    ssize_t total = 0;
    for (auto _ : state) {
        BuildOwningRows();
        total += VectorSize;
    }
    state.SetItemsProcessed(total);
}

void BM_CompactUnversionedOwningRowBuild(benchmark::State& state)
{
    ssize_t total = 0;
    for (auto _ : state) {
        BuildCompactRows();
        total += VectorSize;
    }
    state.SetItemsProcessed(total);
}

void BM_CompactUnversionedOwningRowBuildWithFunctor(benchmark::State& state)
{
    ssize_t total = 0;
    for (auto _ : state) {
        BuildCompactRowsWithFunctor();
        total += VectorSize;
    }
    state.SetItemsProcessed(total);
}

void BM_CompactUnversionedOwningRowBuildWithMake(benchmark::State& state)
{
    ssize_t total = 0;
    for (auto _ : state) {
        BuildCompactRowsWithMake();
        total += VectorSize;
    }
    state.SetItemsProcessed(total);
}

void BM_UnversionedOwningRowCopy(benchmark::State& state)
{
    ssize_t total = 0;
    for (auto _ : state) {
        std::vector<TUnversionedOwningRow> copy = OwningRows;
        benchmark::DoNotOptimize(copy);
        total += VectorSize;
    }
    state.SetItemsProcessed(total);
}

void BM_CompactUnversionedOwningRowCopy(benchmark::State& state)
{
    ssize_t total = 0;
    for (auto _ : state) {
        std::vector<TCompactUnversionedOwningRow> copy = CompactRows;
        benchmark::DoNotOptimize(copy);
        total += VectorSize;
    }
    state.SetItemsProcessed(total);
}

void BM_UnversionedOwningRowGetSpaceUsed(benchmark::State& state)
{
    ssize_t total = 0;
    for (auto _ : state) {
        for (const auto& row : OwningRows) {
            auto x = row.GetSpaceUsed();
            benchmark::DoNotOptimize(x);
        }
        total += OwningRows.size();
    }
    state.SetItemsProcessed(total);
}

void BM_CompactUnversionedOwningRowGetSpaceUsed(benchmark::State& state)
{
    ssize_t total = 0;
    for (auto _ : state) {
        for (const auto& row : CompactRows) {
            auto x = row.GetSpaceUsed();
            benchmark::DoNotOptimize(x);
        }
        total += CompactRows.size();
    }
    state.SetItemsProcessed(total);
}

BENCHMARK(BM_UnversionedOwningRowBuild);
BENCHMARK(BM_CompactUnversionedOwningRowBuild);
BENCHMARK(BM_CompactUnversionedOwningRowBuildWithFunctor);
BENCHMARK(BM_CompactUnversionedOwningRowBuildWithMake);

BENCHMARK(BM_UnversionedOwningRowCopy);
BENCHMARK(BM_CompactUnversionedOwningRowCopy);

BENCHMARK(BM_UnversionedOwningRowGetSpaceUsed);
BENCHMARK(BM_CompactUnversionedOwningRowGetSpaceUsed);

} // namespace
} // namespace NYT::NFlow
