#include <yt/yt/library/query/base/expr_builder_base.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_common.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/test_framework/framework.h>

#include <array>
#include <limits>
#include <string>

namespace NYT::NQueryClient {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

struct TModuloCase
{
    TValue Lhs;
    TValue Rhs;
    TValue Expected;
};

class TModuloTest
    : public ::testing::TestWithParam<TModuloCase>
{ };

TEST_P(TModuloTest, Direct)
{
    const auto& testCase = GetParam();
    EXPECT_EQ(EvaluateModulo(testCase.Lhs, testCase.Rhs), testCase.Expected);
}

TEST_P(TModuloTest, Folded)
{
    const auto& testCase = GetParam();
    auto result = FoldConstants(
        EBinaryOp::Modulo,
        New<TLiteralExpression>(testCase.Lhs.Type, TOwningValue(testCase.Lhs)),
        New<TLiteralExpression>(testCase.Rhs.Type, TOwningValue(testCase.Rhs)));
    ASSERT_TRUE(result);
    EXPECT_EQ(*result, testCase.Expected);
}

TEST_P(TModuloTest, DoesNotInheritOperandMetadata)
{
    const auto& testCase = GetParam();
    auto lhs = testCase.Lhs;
    lhs.Id = 7;
    lhs.Flags = EValueFlags::Aggregate;
    auto rhs = testCase.Rhs;
    rhs.Id = 3;
    rhs.Flags = EValueFlags::Aggregate;

    auto folded = FoldConstants(
        EBinaryOp::Modulo,
        New<TLiteralExpression>(lhs.Type, TOwningValue(lhs)),
        New<TLiteralExpression>(rhs.Type, TOwningValue(rhs)));
    ASSERT_TRUE(folded);

    for (const auto& result : {EvaluateModulo(lhs, rhs), *folded}) {
        EXPECT_EQ(result, testCase.Expected);
        EXPECT_EQ(result.Id, 0);
        EXPECT_EQ(result.Flags, EValueFlags::None);
    }
}

INSTANTIATE_TEST_SUITE_P(
    ScalarSemantics,
    TModuloTest,
    ::testing::Values(
        TModuloCase{
            MakeUnversionedInt64Value(17),
            MakeUnversionedInt64Value(5),
            MakeUnversionedInt64Value(2),
        },
        TModuloCase{
            MakeUnversionedInt64Value(-17),
            MakeUnversionedInt64Value(5),
            MakeUnversionedInt64Value(-2),
        },
        TModuloCase{
            MakeUnversionedInt64Value(17),
            MakeUnversionedInt64Value(-5),
            MakeUnversionedInt64Value(2),
        },
        TModuloCase{
            MakeUnversionedInt64Value(-17),
            MakeUnversionedInt64Value(-5),
            MakeUnversionedInt64Value(-2),
        },
        TModuloCase{
            MakeUnversionedInt64Value(std::numeric_limits<i64>::min()),
            MakeUnversionedInt64Value(1),
            MakeUnversionedInt64Value(0),
        },
        TModuloCase{
            MakeUnversionedUint64Value(std::numeric_limits<ui64>::max()),
            MakeUnversionedUint64Value(2),
            MakeUnversionedUint64Value(1),
        },
        TModuloCase{
            MakeUnversionedUint64Value(0),
            MakeUnversionedUint64Value(std::numeric_limits<ui64>::max()),
            MakeUnversionedUint64Value(0),
        },
        TModuloCase{
            MakeUnversionedNullValue(),
            MakeUnversionedInt64Value(0),
            MakeUnversionedNullValue(),
        },
        TModuloCase{
            MakeUnversionedNullValue(),
            MakeUnversionedUint64Value(0),
            MakeUnversionedNullValue(),
        },
        TModuloCase{
            MakeUnversionedInt64Value(0),
            MakeUnversionedNullValue(),
            MakeUnversionedNullValue(),
        },
        TModuloCase{
            MakeUnversionedUint64Value(0),
            MakeUnversionedNullValue(),
            MakeUnversionedNullValue(),
        },
        TModuloCase{
            MakeUnversionedNullValue(),
            MakeUnversionedNullValue(),
            MakeUnversionedNullValue(),
        }));

////////////////////////////////////////////////////////////////////////////////

struct TModuloErrorCase
{
    TValue Lhs;
    TValue Rhs;
    std::string Error;
};

class TModuloErrorTest
    : public ::testing::TestWithParam<TModuloErrorCase>
{ };

TEST_P(TModuloErrorTest, Direct)
{
    const auto& testCase = GetParam();
    EXPECT_THROW_WITH_SUBSTRING(EvaluateModulo(testCase.Lhs, testCase.Rhs), testCase.Error);
}

TEST_P(TModuloErrorTest, Folded)
{
    const auto& testCase = GetParam();
    auto lhs = New<TLiteralExpression>(testCase.Lhs.Type, TOwningValue(testCase.Lhs));
    auto rhs = New<TLiteralExpression>(testCase.Rhs.Type, TOwningValue(testCase.Rhs));
    EXPECT_THROW_WITH_SUBSTRING(FoldConstants(EBinaryOp::Modulo, lhs, rhs), testCase.Error);
}

INSTANTIATE_TEST_SUITE_P(
    ScalarSemantics,
    TModuloErrorTest,
    ::testing::Values(
        TModuloErrorCase{
            MakeUnversionedInt64Value(1),
            MakeUnversionedInt64Value(0),
            "Division by zero",
        },
        TModuloErrorCase{
            MakeUnversionedUint64Value(1),
            MakeUnversionedUint64Value(0),
            "Division by zero",
        },
        TModuloErrorCase{
            MakeUnversionedInt64Value(std::numeric_limits<i64>::min()),
            MakeUnversionedInt64Value(-1),
            "Division of INT_MIN by -1",
        }));

////////////////////////////////////////////////////////////////////////////////

TEST(TScalarSemanticsTest, ModuloRejectsInvalidOperandTypes)
{
    auto signedValue = MakeUnversionedInt64Value(1);
    auto unsignedValue = MakeUnversionedUint64Value(1);
    auto doubleValue = MakeUnversionedDoubleValue(1.5);

    EXPECT_THROW_WITH_SUBSTRING(
        EvaluateModulo(signedValue, unsignedValue),
        "Modulo operands have different types");
    EXPECT_THROW_WITH_SUBSTRING(
        EvaluateModulo(unsignedValue, signedValue),
        "Modulo operands have different types");
    EXPECT_THROW_WITH_SUBSTRING(
        EvaluateModulo(doubleValue, doubleValue),
        "Cannot compute modulo for values of type");
}

TEST(TScalarSemanticsTest, ModuloPropagatesNullBeforeTypeChecks)
{
    auto nullValue = MakeUnversionedNullValue();
    auto doubleValue = MakeUnversionedDoubleValue(0);

    EXPECT_EQ(EvaluateModulo(nullValue, doubleValue), nullValue);
    EXPECT_EQ(EvaluateModulo(doubleValue, nullValue), nullValue);
}

TEST(TScalarSemanticsTest, FoldedModuloCoercesMixedIntegerLiterals)
{
    const std::array cases{
        TModuloCase{
            MakeUnversionedInt64Value(-1),
            MakeUnversionedUint64Value(3),
            MakeUnversionedUint64Value(0),
        },
        TModuloCase{
            MakeUnversionedUint64Value(3),
            MakeUnversionedInt64Value(-1),
            MakeUnversionedUint64Value(3),
        },
    };

    for (const auto& testCase : cases) {
        auto result = FoldConstants(
            EBinaryOp::Modulo,
            New<TLiteralExpression>(testCase.Lhs.Type, TOwningValue(testCase.Lhs)),
            New<TLiteralExpression>(testCase.Rhs.Type, TOwningValue(testCase.Rhs)));
        ASSERT_TRUE(result);
        EXPECT_EQ(*result, testCase.Expected);
    }
}

TEST(TScalarSemanticsTest, SignedToUnsignedConversion)
{
    struct TCase
    {
        i64 Input = 0;
        ui64 Expected = 0;
    };

    const std::array cases{
        TCase{0, 0},
        TCase{1, 1},
        TCase{-1, std::numeric_limits<ui64>::max()},
        TCase{std::numeric_limits<i64>::min(), ui64{1} << 63},
        TCase{std::numeric_limits<i64>::max(), 0x7fffffffffffffffULL},
    };

    for (const auto& testCase : cases) {
        SCOPED_TRACE(testCase.Input);
        auto input = MakeUnversionedInt64Value(testCase.Input, /*id*/ 7, EValueFlags::Aggregate);
        TValue result = CastValueWithCheck(input, EValueType::Uint64);
        EXPECT_EQ(result, MakeUnversionedUint64Value(testCase.Expected));
        EXPECT_EQ(result.Id, input.Id);
        EXPECT_EQ(result.Flags, input.Flags);
    }
}

TEST(TScalarSemanticsTest, IdentityAndNullConversion)
{
    const std::array values{
        MakeUnversionedInt64Value(std::numeric_limits<i64>::min()),
        MakeUnversionedInt64Value(std::numeric_limits<i64>::max()),
        MakeUnversionedUint64Value(std::numeric_limits<ui64>::max()),
    };
    for (const auto& value : values) {
        EXPECT_EQ(static_cast<TValue>(CastValueWithCheck(value, value.Type)), value);
    }

    auto nullValue = MakeUnversionedNullValue(/*id*/ 7, EValueFlags::Aggregate);
    for (auto targetType : {EValueType::Int64, EValueType::Uint64}) {
        TValue result = CastValueWithCheck(nullValue, targetType);
        EXPECT_EQ(result.Type, EValueType::Null);
        EXPECT_EQ(result.Id, nullValue.Id);
        EXPECT_EQ(result.Flags, nullValue.Flags);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
