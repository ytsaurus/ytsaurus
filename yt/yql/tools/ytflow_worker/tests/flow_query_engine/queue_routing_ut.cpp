#include <yt/yt/flow/library/cpp/connectors/queue/tablet_index_evaluator.h>

#include <yt/yt/flow/library/cpp/common/payload.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/test_framework/framework.h>

#include <optional>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TQueueModuloRoutingTest
    : public ::testing::TestWithParam<EValueType>
{
protected:
    const TTableSchemaPtr Schema_ = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("value", GetParam()),
        TColumnSchema("divisor", GetParam()),
    });
    const TTabletIndexEvaluatorPtr Evaluator_ = New<TTabletIndexEvaluator>(
        Schema_,
        GetParam() == EValueType::Int64 ? "uint64(value % divisor)" : "value % divisor",
        /*policy*/ std::nullopt);

    TPayload CreatePayload(std::optional<i64> value, std::optional<i64> divisor) const
    {
        TPayloadBuilder builder(Schema_);
        builder.SetValue(CreateValue(value, /*id*/ 0));
        builder.SetValue(CreateValue(divisor, /*id*/ 1));
        return builder.Finish();
    }

private:
    TUnversionedValue CreateValue(std::optional<i64> value, int id) const
    {
        if (!value) {
            return MakeUnversionedNullValue(id);
        }
        if (GetParam() == EValueType::Int64) {
            return MakeUnversionedInt64Value(*value, id);
        }
        return MakeUnversionedUint64Value(static_cast<ui64>(*value), id);
    }
};

TEST_P(TQueueModuloRoutingTest, RoutesModuloResult)
{
    EXPECT_EQ(
        Evaluator_->GetTabletIndex(
            CreatePayload(/*value*/ 17, /*divisor*/ 5),
            /*tabletCount*/ 5),
        2);
    EXPECT_EQ(
        Evaluator_->GetTabletIndex(
            CreatePayload(/*value*/ 10, /*divisor*/ 5),
            /*tabletCount*/ 1),
        0);

    if (GetParam() == EValueType::Int64) {
        EXPECT_EQ(
            Evaluator_->GetTabletIndex(
                CreatePayload(/*value*/ 17, /*divisor*/ -5),
                /*tabletCount*/ 5),
            2);
    }
}

TEST_P(TQueueModuloRoutingTest, RejectsNullDividendBeforeDivisionByZero)
{
    for (i64 divisor : {5, 0}) {
        EXPECT_THROW_WITH_SUBSTRING(
            Evaluator_->GetTabletIndex(
                CreatePayload(/*value*/ std::nullopt, divisor),
                /*tabletCount*/ 5),
            "produced a \"null\" value, expected uint64");
    }
}

TEST_P(TQueueModuloRoutingTest, RejectsNullDivisor)
{
    EXPECT_THROW_WITH_SUBSTRING(
        Evaluator_->GetTabletIndex(
            CreatePayload(/*value*/ 17, /*divisor*/ std::nullopt),
            /*tabletCount*/ 5),
        "produced a \"null\" value, expected uint64");
}

TEST_P(TQueueModuloRoutingTest, RejectsZeroDivisorAndCanEvaluateNextPayload)
{
    EXPECT_THROW_WITH_SUBSTRING(
        Evaluator_->GetTabletIndex(
            CreatePayload(/*value*/ 17, /*divisor*/ 0),
            /*tabletCount*/ 5),
        "Division by zero");
    EXPECT_EQ(
        Evaluator_->GetTabletIndex(
            CreatePayload(/*value*/ 17, /*divisor*/ 5),
            /*tabletCount*/ 5),
        2);
}

TEST_P(TQueueModuloRoutingTest, RejectsInvalidModuloResult)
{
    EXPECT_THROW_WITH_SUBSTRING(
        Evaluator_->GetTabletIndex(
            CreatePayload(/*value*/ 8, /*divisor*/ 5),
            /*tabletCount*/ 3),
        "produced tablet index 3 outside the valid range [0, 3)");

    if (GetParam() == EValueType::Int64) {
        EXPECT_THROW_WITH_SUBSTRING(
            Evaluator_->GetTabletIndex(
                CreatePayload(/*value*/ -17, /*divisor*/ 5),
                /*tabletCount*/ 5),
            "does not fit into an int64 $tablet_index");
    }
}

INSTANTIATE_TEST_SUITE_P(
    TQueueRouting,
    TQueueModuloRoutingTest,
    ::testing::Values(EValueType::Int64, EValueType::Uint64));

TEST(TQueueRoutingExpressionTest, RejectsSignedModuloWithoutUint64Cast)
{
    auto schema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("signed_value", EValueType::Int64),
        TColumnSchema("signed_divisor", EValueType::Int64),
    });

    EXPECT_THROW_WITH_SUBSTRING(
        New<TTabletIndexEvaluator>(
            schema,
            "signed_value % signed_divisor",
            /*policy*/ std::nullopt),
        "must produce a uint64");
}

TEST(TQueueRoutingExpressionTest, RoutesHashModuloWithUnusedAnyColumn)
{
    auto schema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("order_id", EValueType::Uint64),
        TColumnSchema("blob", EValueType::Any),
    });
    auto evaluator = New<TTabletIndexEvaluator>(
        schema,
        "farm_hash(order_id) % 100",
        /*policy*/ std::nullopt);

    TPayloadBuilder builder(schema);
    builder.SetValue(MakeUnversionedUint64Value(456, /*id*/ 0));

    EXPECT_EQ(evaluator->GetTabletIndex(builder.Finish(), /*tabletCount*/ 100), /*farm_hash(456u) % 100*/ 19);
}

TEST(TQueueRoutingExpressionTest, RejectsStringModuloOperand)
{
    auto schema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("data", EValueType::String),
    });

    EXPECT_THROW_WITH_SUBSTRING(
        New<TTabletIndexEvaluator>(
            schema,
            "data % 5",
            /*policy*/ std::nullopt),
        "Type mismatch in expression");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
