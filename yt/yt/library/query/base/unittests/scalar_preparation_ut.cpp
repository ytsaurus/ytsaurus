#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/yt/string/format.h>

#include <array>
#include <iterator>
#include <limits>
#include <string>
#include <utility>

namespace NYT::NQueryClient {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void ExpectReference(
    const TConstExpressionPtr& expression,
    const std::string& name,
    EValueType type)
{
    const auto* reference = expression->As<TReferenceExpression>();
    ASSERT_TRUE(reference);
    EXPECT_EQ(name, reference->ColumnName);
    EXPECT_EQ(type, reference->GetWireType());
}

class TScalarPreparationTest
    : public ::testing::TestWithParam<int>
{
protected:
    TConstExpressionPtr Prepare(TStringBuf source, const TTableSchema& schema) const
    {
        auto parsedSource = ParseSource(source, EParseMode::Expression);
        return PrepareExpression(*parsedSource, schema, GetParam());
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_P(TScalarPreparationTest, FarmHashPreservesArgumentTypes)
{
    TTableSchema schema({
        TColumnSchema("i", EValueType::Int64),
        TColumnSchema("u", EValueType::Uint64),
        TColumnSchema("s", EValueType::String),
        TColumnSchema("b", EValueType::Boolean),
    });
    auto expression = Prepare("farm_hash(i, u, s, b)", schema);
    const auto* function = expression->As<TFunctionExpression>();
    ASSERT_TRUE(function);
    EXPECT_EQ("farm_hash", function->FunctionName);
    EXPECT_EQ(EValueType::Uint64, function->GetWireType());
    ASSERT_EQ(4, std::ssize(function->Arguments));
    ExpectReference(function->Arguments[0], "i", EValueType::Int64);
    ExpectReference(function->Arguments[1], "u", EValueType::Uint64);
    ExpectReference(function->Arguments[2], "s", EValueType::String);
    ExpectReference(function->Arguments[3], "b", EValueType::Boolean);
}

TEST_P(TScalarPreparationTest, FarmHashRejectsStaticNullArgument)
{
    TTableSchema schema({TColumnSchema("n", EValueType::Null)});
    auto expectedError = GetParam() == 1
        ? "Wrong type for repeated argument to function"
        : "No matching function";
    EXPECT_THROW_WITH_SUBSTRING(Prepare("farm_hash(n)", schema), expectedError);
}

TEST_P(TScalarPreparationTest, FarmHashNullLiteralDependsOnBuilderVersion)
{
    if (GetParam() == 2) {
        EXPECT_THROW_WITH_SUBSTRING(Prepare("farm_hash(NULL)", /*schema*/ {}), "No matching function");
        return;
    }

    auto expression = Prepare("farm_hash(NULL)", /*schema*/ {});
    const auto* function = expression->As<TFunctionExpression>();
    ASSERT_TRUE(function);
    EXPECT_EQ("farm_hash", function->FunctionName);
    EXPECT_EQ(EValueType::Uint64, function->GetWireType());
    ASSERT_EQ(1, std::ssize(function->Arguments));

    const auto* literal = function->Arguments[0]->As<TLiteralExpression>();
    ASSERT_TRUE(literal);
    EXPECT_EQ(EValueType::Int64, literal->GetWireType());
    EXPECT_EQ(EValueType::Null, literal->Value.Type());
}

TEST_P(TScalarPreparationTest, ModuloPreservesHomogeneousArgumentTypes)
{
    for (auto type : {EValueType::Int64, EValueType::Uint64}) {
        SCOPED_TRACE(Format("%v", type));
        TTableSchema schema({
            TColumnSchema("lhs", type),
            TColumnSchema("rhs", type),
        });
        auto expression = Prepare("lhs % rhs", schema);
        const auto* binary = expression->As<TBinaryOpExpression>();
        ASSERT_TRUE(binary);
        EXPECT_EQ(EBinaryOp::Modulo, binary->Opcode);
        EXPECT_EQ(type, binary->GetWireType());
        ExpectReference(binary->Lhs, "lhs", type);
        ExpectReference(binary->Rhs, "rhs", type);
    }
}

TEST_P(TScalarPreparationTest, ModuloCoercesSignedLiteralToUint64)
{
    TTableSchema schema({TColumnSchema("u", EValueType::Uint64)});
    const std::array cases{
        std::pair{"u % 3", ui64{3}},
        std::pair{"u % -1", std::numeric_limits<ui64>::max()},
    };
    for (const auto& [source, expected] : cases) {
        SCOPED_TRACE(source);
        auto expression = Prepare(source, schema);
        const auto* binary = expression->As<TBinaryOpExpression>();
        ASSERT_TRUE(binary);
        EXPECT_EQ(EBinaryOp::Modulo, binary->Opcode);
        EXPECT_EQ(EValueType::Uint64, binary->GetWireType());
        ExpectReference(binary->Lhs, "u", EValueType::Uint64);

        const auto* literal = binary->Rhs->As<TLiteralExpression>();
        ASSERT_TRUE(literal);
        EXPECT_EQ(EValueType::Uint64, literal->GetWireType());
        auto value = static_cast<TValue>(literal->Value);
        EXPECT_EQ(EValueType::Uint64, value.Type);
        EXPECT_EQ(expected, value.Data.Uint64);
    }
}

TEST_P(TScalarPreparationTest, MixedModuloReferencesDependOnBuilderVersion)
{
    TTableSchema schema({
        TColumnSchema("i", EValueType::Int64),
        TColumnSchema("u", EValueType::Uint64),
    });
    for (bool unsignedFirst : {false, true}) {
        auto source = unsignedFirst ? "u % i" : "i % u";
        SCOPED_TRACE(source);
        if (GetParam() == 1) {
            EXPECT_THROW_WITH_SUBSTRING(Prepare(source, schema), "Type mismatch in expression");
            continue;
        }

        auto expression = Prepare(source, schema);
        const auto* binary = expression->As<TBinaryOpExpression>();
        ASSERT_TRUE(binary);
        EXPECT_EQ(EBinaryOp::Modulo, binary->Opcode);
        EXPECT_EQ(EValueType::Uint64, binary->GetWireType());
        ExpectReference(unsignedFirst ? binary->Lhs : binary->Rhs, "u", EValueType::Uint64);

        const auto& signedOperand = unsignedFirst ? binary->Rhs : binary->Lhs;
        const auto* cast = signedOperand->As<TFunctionExpression>();
        ASSERT_TRUE(cast);
        EXPECT_EQ("uint64", cast->FunctionName);
        EXPECT_EQ(EValueType::Uint64, cast->GetWireType());
        ASSERT_EQ(1, std::ssize(cast->Arguments));
        ExpectReference(cast->Arguments[0], "i", EValueType::Int64);
    }
}

TEST_P(TScalarPreparationTest, ModuloCoercesNullLiteralToInteger)
{
    for (auto type : {EValueType::Int64, EValueType::Uint64}) {
        SCOPED_TRACE(Format("%v", type));
        TTableSchema schema({TColumnSchema("value", type)});
        for (bool nullFirst : {false, true}) {
            auto source = nullFirst ? "NULL % value" : "value % NULL";
            SCOPED_TRACE(source);
            auto expression = Prepare(source, schema);
            const auto* binary = expression->As<TBinaryOpExpression>();
            ASSERT_TRUE(binary);
            EXPECT_EQ(EBinaryOp::Modulo, binary->Opcode);
            EXPECT_EQ(type, binary->GetWireType());
            ExpectReference(nullFirst ? binary->Rhs : binary->Lhs, "value", type);

            const auto& nullOperand = nullFirst ? binary->Lhs : binary->Rhs;
            const auto* literal = nullOperand->As<TLiteralExpression>();
            ASSERT_TRUE(literal);
            EXPECT_EQ(type, literal->GetWireType());
            EXPECT_EQ(EValueType::Null, literal->Value.Type());
        }
    }
}

TEST_P(TScalarPreparationTest, ModuloWithStaticNullArgumentDependsOnBuilderVersion)
{
    for (auto type : {EValueType::Int64, EValueType::Uint64}) {
        SCOPED_TRACE(Format("%v", type));
        TTableSchema schema({
            TColumnSchema("n", EValueType::Null),
            TColumnSchema("value", type),
        });
        for (bool nullFirst : {false, true}) {
            auto source = nullFirst ? "n % value" : "value % n";
            SCOPED_TRACE(source);
            if (GetParam() == 1) {
                EXPECT_THROW_WITH_SUBSTRING(Prepare(source, schema), "Type mismatch in expression");
                continue;
            }

            auto expression = Prepare(source, schema);
            const auto* binary = expression->As<TBinaryOpExpression>();
            ASSERT_TRUE(binary);
            EXPECT_EQ(EBinaryOp::Modulo, binary->Opcode);
            EXPECT_EQ(type, binary->GetWireType());
            ExpectReference(nullFirst ? binary->Rhs : binary->Lhs, "value", type);

            const auto& nullOperand = nullFirst ? binary->Lhs : binary->Rhs;
            const auto* cast = nullOperand->As<TFunctionExpression>();
            ASSERT_TRUE(cast);
            EXPECT_EQ(type == EValueType::Int64 ? "int64" : "uint64", cast->FunctionName);
            EXPECT_EQ(type, cast->GetWireType());
            ASSERT_EQ(1, std::ssize(cast->Arguments));
            ExpectReference(cast->Arguments[0], "n", EValueType::Null);
        }
    }
}

TEST_P(TScalarPreparationTest, Uint64CastPreservesStaticNullArgument)
{
    TTableSchema schema({TColumnSchema("n", EValueType::Null)});
    for (const auto* source : {"uint64(n)", "uint64(NULL)"}) {
        SCOPED_TRACE(source);
        auto expression = Prepare(source, schema);
        const auto* cast = expression->As<TFunctionExpression>();
        ASSERT_TRUE(cast);
        EXPECT_EQ("uint64", cast->FunctionName);
        EXPECT_EQ(EValueType::Uint64, cast->GetWireType());
        ASSERT_EQ(1, std::ssize(cast->Arguments));
        EXPECT_EQ(EValueType::Null, cast->Arguments[0]->GetWireType());
    }
}

TEST_P(TScalarPreparationTest, FarmHashModuloSignedReferenceDependsOnBuilderVersion)
{
    TTableSchema schema({
        TColumnSchema("key", EValueType::String),
        TColumnSchema("divisor", EValueType::Int64),
    });
    if (GetParam() == 1) {
        EXPECT_THROW_WITH_SUBSTRING(
            Prepare("farm_hash(key) % divisor", schema),
            "Type mismatch in expression");
        return;
    }

    auto expression = Prepare("farm_hash(key) % divisor", schema);
    const auto* binary = expression->As<TBinaryOpExpression>();
    ASSERT_TRUE(binary);
    EXPECT_EQ(EBinaryOp::Modulo, binary->Opcode);
    EXPECT_EQ(EValueType::Uint64, binary->GetWireType());

    const auto* hash = binary->Lhs->As<TFunctionExpression>();
    ASSERT_TRUE(hash);
    EXPECT_EQ("farm_hash", hash->FunctionName);
    EXPECT_EQ(EValueType::Uint64, hash->GetWireType());
    ASSERT_EQ(1, std::ssize(hash->Arguments));
    ExpectReference(hash->Arguments[0], "key", EValueType::String);

    const auto* cast = binary->Rhs->As<TFunctionExpression>();
    ASSERT_TRUE(cast);
    EXPECT_EQ("uint64", cast->FunctionName);
    EXPECT_EQ(EValueType::Uint64, cast->GetWireType());
    ASSERT_EQ(1, std::ssize(cast->Arguments));
    ExpectReference(cast->Arguments[0], "divisor", EValueType::Int64);
}

TEST_P(TScalarPreparationTest, ModuloRejectsDoubleArguments)
{
    TTableSchema schema({
        TColumnSchema("i", EValueType::Int64),
        TColumnSchema("d", EValueType::Double),
    });
    for (const auto* source : {"d % i", "i % d", "d % d"}) {
        SCOPED_TRACE(source);
        EXPECT_THROW_WITH_SUBSTRING(Prepare(source, schema), "Type mismatch in expression");
    }
}

INSTANTIATE_TEST_SUITE_P(BuilderVersions, TScalarPreparationTest, ::testing::Values(1, 2));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
