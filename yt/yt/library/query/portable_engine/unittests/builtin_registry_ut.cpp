#include <yt/yt/library/query/portable_engine/builtin_registry.h>
#include <yt/yt/library/query/portable_engine/program.h>
#include <yt/yt/library/query/portable_engine/registry.h>

#include <yt/yt/library/query/base/expr_builder_base.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/yt/string/format.h>

#include <array>
#include <vector>

namespace NYT::NQueryClient::NPortable {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TEST(TBuiltinExpressionRegistryTest, StableInstance)
{
    EXPECT_EQ(&GetBuiltinExpressionRegistry(), &GetBuiltinExpressionRegistry());
}

TEST(TBuiltinExpressionRegistryTest, SupportedSignatures)
{
    const auto& registry = GetBuiltinExpressionRegistry();
    const std::array hashTypes{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Boolean,
        EValueType::String,
    };
    for (auto type : hashTypes) {
        auto operation = registry.FindFunction("farm_hash", {type}, EValueType::Uint64);
        ASSERT_TRUE(operation);
        EXPECT_EQ(1, operation->ArgumentCount);
        EXPECT_EQ(EOperationNullPolicy::PassToCallback, operation->Implementation.NullPolicy);
    }

    auto hash = registry.FindFunction("farm_hash", hashTypes, EValueType::Uint64);
    ASSERT_TRUE(hash);
    EXPECT_EQ(4, hash->ArgumentCount);

    for (auto type : {EValueType::Int64, EValueType::Uint64}) {
        auto operation = registry.FindBinary(EBinaryOp::Modulo, type, type, type);
        ASSERT_TRUE(operation);
        EXPECT_EQ(2, operation->ArgumentCount);
        EXPECT_EQ(EOperationNullPolicy::Propagate, operation->Implementation.NullPolicy);
    }

    auto cast = registry.FindFunction("uint64", {EValueType::Int64}, EValueType::Uint64);
    ASSERT_TRUE(cast);
    EXPECT_EQ(1, cast->ArgumentCount);
    EXPECT_EQ(EOperationNullPolicy::Propagate, cast->Implementation.NullPolicy);
}

TEST(TBuiltinExpressionRegistryTest, UnsupportedSignatures)
{
    const auto& registry = GetBuiltinExpressionRegistry();
    EXPECT_FALSE(registry.FindFunction("farm_hash", /*argumentTypes*/ {}, EValueType::Uint64));
    EXPECT_FALSE(registry.FindFunction("farm_hash", {EValueType::Int64}, EValueType::Int64));
    for (auto type : {EValueType::Null, EValueType::Double, EValueType::Any}) {
        EXPECT_FALSE(registry.FindFunction("farm_hash", {type}, EValueType::Uint64));
        EXPECT_FALSE(registry.FindFunction("uint64", {type}, EValueType::Uint64));
        EXPECT_FALSE(registry.FindBinary(EBinaryOp::Modulo, type, type, type));
        EXPECT_FALSE(registry.FindBinary(EBinaryOp::Modulo, type, EValueType::Int64, EValueType::Int64));
    }

    EXPECT_FALSE(registry.FindFunction("uint64", {EValueType::Uint64}, EValueType::Uint64));
    EXPECT_FALSE(registry.FindFunction("uint64", {EValueType::Int64}, EValueType::Int64));
    EXPECT_FALSE(registry.FindBinary(EBinaryOp::Modulo, EValueType::Int64, EValueType::Uint64, EValueType::Uint64));
    EXPECT_FALSE(registry.FindBinary(EBinaryOp::Plus, EValueType::Int64, EValueType::Int64, EValueType::Int64));
    EXPECT_FALSE(registry.FindUnary(EUnaryOp::Minus, EValueType::Int64, EValueType::Int64));
    EXPECT_FALSE(registry.FindFunction("is_null", {EValueType::Int64}, EValueType::Boolean));
}

TEST(TBuiltinExpressionRegistryTest, UnsupportedExpressionsFailAtCompilation)
{
    TTableSchema schema({
        TColumnSchema("i", EValueType::Int64),
        TColumnSchema("n", EValueType::Null),
    });
    for (const auto* source : {"farm_hash()", "i + i", "uint64(n)", "if(true, i, i)"}) {
        SCOPED_TRACE(source);
        auto expression = PrepareExpression(source, schema);
        EXPECT_THROW_WITH_SUBSTRING(
            CompileExpression(expression, schema, GetBuiltinExpressionRegistry()),
            "Unsupported portable");
    }
}

TEST(TBuiltinExpressionRegistryTest, ModuloPreservesOutputMetadataWithAndWithoutFolding)
{
    for (auto type : {EValueType::Int64, EValueType::Uint64}) {
        SCOPED_TRACE(Format("Type: %v", type));
        auto makeValue = [&] (i64 value) {
            return type == EValueType::Int64
                ? MakeUnversionedInt64Value(value)
                : MakeUnversionedUint64Value(value);
        };
        const std::array cases{
            std::array{makeValue(17), makeValue(5)},
            std::array{MakeUnversionedNullValue(), makeValue(5)},
            std::array{makeValue(17), MakeUnversionedNullValue()},
            std::array{MakeUnversionedNullValue(), MakeUnversionedNullValue()},
        };
        for (auto [lhs, rhs] : cases) {
            SCOPED_TRACE(Format("LhsType: %v, RhsType: %v", lhs.Type, rhs.Type));
            lhs.Id = 7;
            lhs.Flags = EValueFlags::Aggregate;
            rhs.Id = 3;
            rhs.Flags = EValueFlags::Aggregate;

            auto lhsExpression = New<TLiteralExpression>(type, TOwningValue(lhs));
            auto rhsExpression = New<TLiteralExpression>(type, TOwningValue(rhs));
            auto folded = FoldConstants(EBinaryOp::Modulo, lhsExpression, rhsExpression);
            ASSERT_TRUE(folded);

            const std::array<TConstExpressionPtr, 2> expressions{
                New<TBinaryOpExpression>(type, EBinaryOp::Modulo, lhsExpression, rhsExpression),
                New<TLiteralExpression>(type, TOwningValue(*folded)),
            };
            auto expected = lhs.Type == EValueType::Null || rhs.Type == EValueType::Null
                ? MakeUnversionedNullValue()
                : makeValue(2);
            for (const auto& expression : expressions) {
                SCOPED_TRACE(expression->As<TLiteralExpression>() ? "folded" : "unfolded");
                auto program = CompileExpression(expression, /*schema*/ {}, GetBuiltinExpressionRegistry());
                std::vector<TValue> scratch(program.GetScratchValueCount());
                auto rowBuffer = New<TRowBuffer>();
                for (auto flags : {EValueFlags::None, EValueFlags::Aggregate}) {
                    SCOPED_TRACE(Format("Flags: %v", flags));
                    auto result = MakeUnversionedNullValue(/*id*/ 12, flags);
                    program.Evaluate(&result, /*inputRow*/ {}, scratch, rowBuffer);

                    EXPECT_EQ(expected, result);
                    EXPECT_EQ(12, result.Id);
                    EXPECT_EQ(flags, result.Flags);
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient::NPortable
