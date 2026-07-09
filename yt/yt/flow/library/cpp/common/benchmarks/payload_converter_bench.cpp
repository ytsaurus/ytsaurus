#include <benchmark/benchmark.h>

#include <yt/yt/flow/library/cpp/common/column_evaluator_cache.h>
#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/payload_converter.h>
#include <yt/yt/flow/library/cpp/common/schema.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

constexpr int VectorSize = 10000;

////////////////////////////////////////////////////////////////////////////////

NTableClient::TTableSchemaPtr MakeSubsetSourceSchema()
{
    return ConvertTo<TTableSchemaPtr>(NYson::TYsonString(TStringBuf(R""""(
        [
            {name="order_id";   type="uint64";};
            {name="banner_id";  type="uint64";};
            {name="is_click";   type="boolean";};
            {name="price";      type="uint64";};
            {name="region_id";  type="uint64";};
        ]
    )"""")));
}

NTableClient::TTableSchemaPtr MakeSubsetTargetSchema()
{
    return ConvertTo<TTableSchemaPtr>(NYson::TYsonString(TStringBuf(R""""(
        [
            {name="order_id";  type="uint64";};
            {name="banner_id"; type="uint64";};
            {name="is_click";  type="boolean";};
        ]
    )"""")));
}

NTableClient::TTableSchemaPtr MakeComputeKeySourceSchema()
{
    return ConvertTo<TTableSchemaPtr>(NYson::TYsonString(TStringBuf(R""""(
        [
            {name="order_id";  type="uint64";};
            {name="banner_id"; type="uint64";};
        ]
    )"""")));
}

NTableClient::TTableSchemaPtr MakeComputeKeyTargetSchema()
{
    return ConvertTo<TTableSchemaPtr>(NYson::TYsonString(TStringBuf(R"""(
        [
            {name="hash";      type="uint64"; expression="farm_hash(order_id)"; sort_order="ascending";};
            {name="order_id";  type="uint64"; sort_order="ascending";};
            {name="banner_id"; type="uint64";};
        ]
    )""")));
}

std::vector<TPayload> BuildSubsetPayloads(const TTableSchemaPtr& schema)
{
    std::vector<TPayload> payloads;
    payloads.reserve(VectorSize);
    for (int i = 0; i < VectorSize; ++i) {
        TPayloadBuilder builder(schema);
        builder.SetValue(MakeUnversionedUint64Value(i, 0));
        builder.SetValue(MakeUnversionedUint64Value(i * 2, 1));
        builder.SetValue(MakeUnversionedBooleanValue(i % 2 == 0, 2));
        builder.SetValue(MakeUnversionedUint64Value(i * 100, 3));
        builder.SetValue(MakeUnversionedUint64Value(i % 1000, 4));
        payloads.push_back(builder.Finish());
    }
    return payloads;
}

std::vector<TPayload> BuildComputeKeyPayloads(const TTableSchemaPtr& schema)
{
    std::vector<TPayload> payloads;
    payloads.reserve(VectorSize);
    for (int i = 0; i < VectorSize; ++i) {
        TPayloadBuilder builder(schema);
        builder.SetValue(MakeUnversionedUint64Value(i, 0));
        builder.SetValue(MakeUnversionedUint64Value(i * 2, 1));
        payloads.push_back(builder.Finish());
    }
    return payloads;
}

////////////////////////////////////////////////////////////////////////////////
// Fixture: holds all ref-counted objects and payloads.
// Initialized once per benchmark run, destroyed before process exit.
////////////////////////////////////////////////////////////////////////////////

class TConvertPayloadFixture
    : public benchmark::Fixture
{
public:
    void SetUp(benchmark::State& /*state*/) override
    {
        auto guard = Guard(InitLock_);
        ++ThreadReferenceCounter_;
        if (ThreadReferenceCounter_ != 1) {
            return;
        }

        SubsetSourceSchema_ = MakeSubsetSourceSchema();
        SubsetTargetSchema_ = MakeSubsetTargetSchema();
        ComputeKeySourceSchema_ = MakeComputeKeySourceSchema();
        ComputeKeyTargetSchema_ = MakeComputeKeyTargetSchema();

        SubsetPayloads_ = BuildSubsetPayloads(SubsetSourceSchema_);
        ComputeKeyPayloads_ = BuildComputeKeyPayloads(ComputeKeySourceSchema_);

        ConverterCache_ = CreatePayloadConverterCache(CreateFastColumnEvaluatorCache());
    }

    void TearDown(benchmark::State& /*state*/) override
    {
        auto guard = Guard(InitLock_);
        --ThreadReferenceCounter_;
        if (ThreadReferenceCounter_ != 0) {
            return;
        }

        SubsetPayloads_.clear();
        ComputeKeyPayloads_.clear();
        SubsetSourceSchema_.Reset();
        SubsetTargetSchema_.Reset();
        ComputeKeySourceSchema_.Reset();
        ComputeKeyTargetSchema_.Reset();
        ConverterCache_.Reset();
    }

protected:
    TTableSchemaPtr SubsetSourceSchema_;
    TTableSchemaPtr SubsetTargetSchema_;
    TTableSchemaPtr ComputeKeySourceSchema_;
    TTableSchemaPtr ComputeKeyTargetSchema_;

    std::vector<TPayload> SubsetPayloads_;
    std::vector<TPayload> ComputeKeyPayloads_;

    IPayloadConverterCachePtr ConverterCache_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, InitLock_);
    int ThreadReferenceCounter_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

// clang-format off

// Case 1: subset of columns.
BENCHMARK_F(TConvertPayloadFixture, BM_ConvertPayloadSubsetColumns)(benchmark::State& state)
{
    ssize_t total = 0;
    for (auto _ : state) {
        for (const auto& payload : SubsetPayloads_) {
            auto result = ConvertPayloadToNewSchema(payload, SubsetSourceSchema_, SubsetTargetSchema_, ConverterCache_);
            benchmark::DoNotOptimize(result);
        }
        total += VectorSize;
    }
    state.SetItemsProcessed(total);
}

BENCHMARK_REGISTER_F(TConvertPayloadFixture, BM_ConvertPayloadSubsetColumns)->Threads(1)->Threads(4);

// Case 2: compute key (with expression column).
BENCHMARK_F(TConvertPayloadFixture, BM_ConvertPayloadComputeKey)(benchmark::State& state)
{
    ssize_t total = 0;
    for (auto _ : state) {
        for (const auto& payload : ComputeKeyPayloads_) {
            auto result = ConvertPayloadToNewSchema(payload, ComputeKeySourceSchema_, ComputeKeyTargetSchema_, ConverterCache_);
            benchmark::DoNotOptimize(result);
        }
        total += VectorSize;
    }
    state.SetItemsProcessed(total);
}

BENCHMARK_REGISTER_F(TConvertPayloadFixture, BM_ConvertPayloadComputeKey)->Threads(1)->Threads(4);

// clang-format on

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
