#include <yt/yt/library/query/portable_engine/program.h>
#include <yt/yt/library/query/portable_engine/registry.h>

#include <yt/yt/library/query/base/functions.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/memory/weak_ptr.h>

#include <library/cpp/yt/string/format.h>

#include <array>
#include <atomic>
#include <barrier>
#include <initializer_list>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace NYT::NQueryClient::NPortable {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TConstExpressionPtr MakeLiteral(TValue value)
{
    return New<TLiteralExpression>(value.Type, TOwningValue(value));
}

TConstExpressionPtr MakeReference(
    const std::string& name,
    EValueType type = EValueType::Int64)
{
    return New<TReferenceExpression>(TColumnSchema(name, type).LogicalType(), name);
}

TConstExpressionPtr MakeFunction(
    const std::string& name,
    std::vector<TConstExpressionPtr> arguments,
    EValueType resultType = EValueType::Int64)
{
    return New<TFunctionExpression>(resultType, name, std::move(arguments));
}

TOperationDescriptor MakeDescriptor(
    std::initializer_list<EValueType> argumentTypes,
    TOperationCallback callback,
    EValueType resultType = EValueType::Int64,
    EOperationNullPolicy nullPolicy = EOperationNullPolicy::Propagate)
{
    return {
        .ArgumentTypes = std::vector<EValueType>(argumentTypes),
        .ResultType = resultType,
        .Implementation = {
            .NullPolicy = nullPolicy,
            .Callback = std::move(callback),
        },
    };
}

void CopyFirstArgument(
    TValue* result,
    TRange<TValue> arguments,
    const TRowBufferPtr& /*rowBuffer*/)
{
    *result = arguments[0];
}

TError GetCompilationError(
    const TConstExpressionPtr& expression,
    const TTableSchema& schema,
    const TExpressionRegistry& registry)
{
    try {
        CompileExpression(expression, schema, registry);
    } catch (const TErrorException& ex) {
        return ex.Error();
    }

    ADD_FAILURE() << "Expected expression compilation to fail";
    return {};
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPortableExpressionProgramTest, ScalarLiterals)
{
    std::array values{
        MakeUnversionedNullValue(),
        MakeUnversionedInt64Value(-17),
        MakeUnversionedUint64Value(42),
        MakeUnversionedDoubleValue(2.5),
        MakeUnversionedBooleanValue(true),
    };

    for (const auto& value : values) {
        auto program = CompileExpression(MakeLiteral(value), /*schema*/ {}, /*registry*/ {});
        EXPECT_EQ(1, program.GetScratchValueCount());
        EXPECT_TRUE(program.GetReferenceIds().empty());

        auto rowBuffer = New<TRowBuffer>();
        std::vector<TValue> scratch(program.GetScratchValueCount());
        auto result = MakeUnversionedNullValue();
        program.Evaluate(&result, /*inputRow*/ {}, scratch, rowBuffer);

        EXPECT_EQ(value.Type, result.Type);
        EXPECT_EQ(ToString(value), ToString(result));
        EXPECT_EQ(0, rowBuffer->GetSize());
    }
}

TEST(TPortableExpressionProgramTest, ReferencesUseSchemaPositionsAndPreserveOutputMetadata)
{
    TTableSchema schema({
        TColumnSchema("unused", EValueType::Int64),
        TColumnSchema("value", EValueType::Int64),
    });
    auto program = CompileExpression(MakeReference("value"), schema, /*registry*/ {});
    EXPECT_EQ(std::vector<int>({1}), program.GetReferenceIds());
    EXPECT_EQ(1, program.GetScratchValueCount());

    std::array row{
        MakeUnversionedInt64Value(11, /*id*/ 1),
        MakeUnversionedInt64Value(42, /*id*/ 57),
    };
    auto result = MakeUnversionedNullValue(/*id*/ 8, EValueFlags::Aggregate);
    auto rowBuffer = New<TRowBuffer>();
    std::vector<TValue> scratch(program.GetScratchValueCount());
    program.Evaluate(&result, row, scratch, rowBuffer);

    EXPECT_EQ(EValueType::Int64, result.Type);
    EXPECT_EQ(42, result.Data.Int64);
    EXPECT_EQ(8, result.Id);
    EXPECT_EQ(EValueFlags::Aggregate, result.Flags);
}

TEST(TPortableExpressionProgramTest, NestedCallsPreserveArgumentOrder)
{
    std::vector<std::string> calls;
    TExpressionRegistryBuilder builder;
    builder.RegisterUnary(
        EUnaryOp::Minus,
        MakeDescriptor(
            {EValueType::Int64},
            BIND([&] (TValue* result, TRange<TValue> arguments, const TRowBufferPtr& /*rowBuffer*/) {
                calls.push_back("negate");
                *result = MakeUnversionedInt64Value(-arguments[0].Data.Int64);
            })));
    builder.RegisterBinary(
        EBinaryOp::Minus,
        MakeDescriptor(
            {EValueType::Int64, EValueType::Int64},
            BIND([&] (TValue* result, TRange<TValue> arguments, const TRowBufferPtr& /*rowBuffer*/) {
                calls.push_back("subtract");
                *result = MakeUnversionedInt64Value(arguments[0].Data.Int64 - arguments[1].Data.Int64);
            })));
    builder.RegisterFunction(
        "combine",
        MakeDescriptor(
            {EValueType::Int64, EValueType::Int64},
            BIND([&] (TValue* result, TRange<TValue> arguments, const TRowBufferPtr& /*rowBuffer*/) {
                calls.push_back("combine");
                *result = MakeUnversionedInt64Value(100 * arguments[0].Data.Int64 + arguments[1].Data.Int64);
            })));

    auto expression = MakeFunction(
        "combine",
        {
            New<TUnaryOpExpression>(EValueType::Int64, EUnaryOp::Minus, MakeReference("x")),
            New<TBinaryOpExpression>(
                EValueType::Int64,
                EBinaryOp::Minus,
                MakeReference("y"),
                MakeLiteral(MakeUnversionedInt64Value(3))),
        });
    TTableSchema schema({
        TColumnSchema("x", EValueType::Int64),
        TColumnSchema("y", EValueType::Int64),
    });
    auto program = CompileExpression(expression, schema, builder.Build());
    EXPECT_EQ(8, program.GetScratchValueCount());

    auto rowBuffer = New<TRowBuffer>();
    std::vector<TValue> scratch(program.GetScratchValueCount());
    std::array row{MakeUnversionedInt64Value(2), MakeUnversionedInt64Value(8)};
    auto result = MakeUnversionedNullValue(/*id*/ 9, EValueFlags::Aggregate);
    program.Evaluate(&result, row, scratch, rowBuffer);

    EXPECT_EQ(-195, result.Data.Int64);
    EXPECT_EQ(std::vector<std::string>({"negate", "subtract", "combine"}), calls);
    EXPECT_EQ(9, result.Id);
    EXPECT_EQ(EValueFlags::Aggregate, result.Flags);
}

TEST(TPortableExpressionProgramTest, CollectsSortedUniqueReferences)
{
    TExpressionRegistryBuilder builder;
    builder.RegisterVariadicFunction(
        "first",
        {
            .AllowedArgumentTypes = TTypeSet({EValueType::Int64}),
            .ResultType = EValueType::Int64,
            .Implementation = {.Callback = BIND(&CopyFirstArgument)},
        });
    TTableSchema schema({
        TColumnSchema("x", EValueType::Int64),
        TColumnSchema("unused", EValueType::Int64),
        TColumnSchema("z", EValueType::Int64),
    });
    auto reference = MakeReference("z");
    auto expression = MakeFunction("first", {reference, MakeReference("x"), reference});
    auto program = CompileExpression(expression, schema, builder.Build());

    EXPECT_EQ(std::vector<int>({0, 2}), program.GetReferenceIds());
    EXPECT_EQ(7, program.GetScratchValueCount());
}

TEST(TPortableExpressionProgramTest, InvokesZeroArgumentFunction)
{
    TExpressionRegistryBuilder builder;
    builder.RegisterFunction(
        "constant",
        MakeDescriptor(
            /*argumentTypes*/ {},
            BIND([] (TValue* result, TRange<TValue> arguments, const TRowBufferPtr& /*rowBuffer*/) {
                EXPECT_TRUE(arguments.empty());
                *result = MakeUnversionedInt64Value(42);
            })));
    auto program = CompileExpression(MakeFunction("constant", /*arguments*/ {}), /*schema*/ {}, builder.Build());
    EXPECT_EQ(1, program.GetScratchValueCount());

    auto rowBuffer = New<TRowBuffer>();
    std::vector<TValue> scratch(program.GetScratchValueCount());
    auto result = MakeUnversionedNullValue();
    program.Evaluate(&result, /*inputRow*/ {}, scratch, rowBuffer);
    EXPECT_EQ(42, result.Data.Int64);
}

TEST(TPortableExpressionProgramTest, NullPoliciesAndScratchReuse)
{
    int propagateCallCount = 0;
    int passCallCount = 0;
    TExpressionRegistryBuilder builder;
    builder.RegisterFunction(
        "propagate",
        MakeDescriptor(
            {EValueType::Int64},
            BIND([&] (TValue* result, TRange<TValue> arguments, const TRowBufferPtr& /*rowBuffer*/) {
                ++propagateCallCount;
                *result = MakeUnversionedInt64Value(arguments[0].Data.Int64 + 1);
            })));
    builder.RegisterFunction(
        "is_null",
        MakeDescriptor(
            {EValueType::Int64},
            BIND([&] (TValue* result, TRange<TValue> arguments, const TRowBufferPtr& /*rowBuffer*/) {
                ++passCallCount;
                *result = MakeUnversionedBooleanValue(arguments[0].Type == EValueType::Null);
            }),
            EValueType::Boolean,
            EOperationNullPolicy::PassToCallback));
    TTableSchema schema({TColumnSchema("x", EValueType::Int64)});
    auto registry = builder.Build();
    auto propagateProgram = CompileExpression(
        MakeFunction("propagate", {MakeReference("x")}),
        schema,
        registry);
    auto passProgram = CompileExpression(
        MakeFunction("is_null", {MakeReference("x")}, EValueType::Boolean),
        schema,
        registry);

    auto rowBuffer = New<TRowBuffer>();
    std::vector<TValue> scratch(propagateProgram.GetScratchValueCount());
    auto result = MakeUnversionedNullValue(/*id*/ 4, EValueFlags::Aggregate);
    std::array values{MakeUnversionedNullValue(), MakeUnversionedInt64Value(16), MakeUnversionedNullValue()};
    for (const auto& value : values) {
        std::array row{value};
        propagateProgram.Evaluate(&result, row, scratch, rowBuffer);
        EXPECT_EQ(value.Type, result.Type);
        if (value.Type != EValueType::Null) {
            EXPECT_EQ(17, result.Data.Int64);
        }
        EXPECT_EQ(4, result.Id);
        EXPECT_EQ(EValueFlags::Aggregate, result.Flags);

        passProgram.Evaluate(&result, row, scratch, rowBuffer);
        EXPECT_EQ(EValueType::Boolean, result.Type);
        EXPECT_EQ(value.Type == EValueType::Null, result.Data.Boolean);
    }
    EXPECT_EQ(1, propagateCallCount);
    EXPECT_EQ(3, passCallCount);
}

TEST(TPortableExpressionProgramTest, NullPropagationDoesNotSkipArguments)
{
    TExpressionRegistryBuilder builder;
    builder.RegisterFunction(
        "outer",
        MakeDescriptor(
            {EValueType::Int64, EValueType::Int64},
            BIND(&CopyFirstArgument),
            EValueType::Int64,
            EOperationNullPolicy::Propagate));
    builder.RegisterFunction(
        "outer",
        MakeDescriptor(
            {EValueType::Null, EValueType::Int64},
            BIND(&CopyFirstArgument),
            EValueType::Int64,
            EOperationNullPolicy::Propagate));
    builder.RegisterFunction(
        "throwing_child",
        MakeDescriptor(
            /*argumentTypes*/ {},
            BIND([] (TValue* /*result*/, TRange<TValue> /*arguments*/, const TRowBufferPtr& /*rowBuffer*/) {
                throw std::runtime_error("child evaluation failed");
            })));
    TTableSchema schema({TColumnSchema("x", EValueType::Int64)});
    auto registry = builder.Build();
    std::array<TConstExpressionPtr, 3> firstArguments{
        MakeReference("x"),
        New<TLiteralExpression>(EValueType::Int64, TOwningValue(MakeUnversionedNullValue())),
        MakeLiteral(MakeUnversionedNullValue()),
    };
    for (const auto& firstArgument : firstArguments) {
        auto program = CompileExpression(
            MakeFunction("outer", {firstArgument, MakeFunction("throwing_child", /*arguments*/ {})}),
            schema,
            registry);

        auto rowBuffer = New<TRowBuffer>();
        std::vector<TValue> scratch(program.GetScratchValueCount());
        std::array row{MakeUnversionedNullValue()};
        auto result = MakeUnversionedNullValue();
        EXPECT_THROW_WITH_SUBSTRING(
            program.Evaluate(&result, row, scratch, rowBuffer),
            "child evaluation failed");
    }
}

TEST(TPortableExpressionProgramTest, PreparedNullLiteralUsesExpectedArgumentType)
{
    auto functions = New<TTypeInferrerMap>();
    functions->emplace(
        "test_identity",
        CreateFunctionTypeInferrer(
            EValueType::Int64,
            std::vector<TType>{EValueType::Int64}));
    auto expression = PrepareExpression("test_identity(NULL)", /*tableSchema*/ {}, functions);
    const auto* function = expression->As<TFunctionExpression>();
    ASSERT_TRUE(function);
    ASSERT_EQ(1, std::ssize(function->Arguments));
    const auto* literal = function->Arguments[0]->As<TLiteralExpression>();
    ASSERT_TRUE(literal);
    EXPECT_EQ(EValueType::Int64, literal->GetWireType());
    EXPECT_EQ(EValueType::Null, literal->Value.Type());

    int callCount = 0;
    TExpressionRegistryBuilder builder;
    builder.RegisterFunction(
        "test_identity",
        MakeDescriptor(
            {EValueType::Int64},
            BIND([&] (TValue* result, TRange<TValue> arguments, const TRowBufferPtr& /*rowBuffer*/) {
                ++callCount;
                *result = arguments[0];
            }),
            EValueType::Int64,
            EOperationNullPolicy::Propagate));
    auto program = CompileExpression(expression, /*schema*/ {}, builder.Build());

    auto rowBuffer = New<TRowBuffer>();
    std::vector<TValue> scratch(program.GetScratchValueCount());
    auto result = MakeUnversionedInt64Value(42);
    program.Evaluate(&result, /*inputRow*/ {}, scratch, rowBuffer);

    EXPECT_EQ(EValueType::Null, result.Type);
    EXPECT_EQ(0, callCount);
}

TEST(TPortableExpressionProgramTest, StaticNullArgumentRequiresExactSignature)
{
    auto expression = PrepareExpression("is_null(NULL)", /*tableSchema*/ {});
    const auto* function = expression->As<TFunctionExpression>();
    ASSERT_TRUE(function);
    ASSERT_EQ(1, std::ssize(function->Arguments));
    EXPECT_EQ(EValueType::Null, function->Arguments[0]->GetWireType());

    int callCount = 0;
    auto callback = BIND([&] (TValue* result, TRange<TValue> arguments, const TRowBufferPtr& /*rowBuffer*/) {
        ++callCount;
        *result = MakeUnversionedBooleanValue(arguments[0].Type == EValueType::Null);
    });
    TExpressionRegistryBuilder builder;
    builder.RegisterFunction(
        "is_null",
        MakeDescriptor({EValueType::Int64}, callback, EValueType::Boolean, EOperationNullPolicy::PassToCallback));

    auto error = GetCompilationError(expression, /*schema*/ {}, builder.Build());
    ASSERT_FALSE(error.IsOK());
    EXPECT_EQ("root", error.Attributes().Get<std::string>("expression_path"));
    EXPECT_EQ("is_null", error.Attributes().Get<std::string>("operation"));
    EXPECT_EQ(
        std::vector<EValueType>({EValueType::Null}),
        error.Attributes().Get<std::vector<EValueType>>("argument_types"));
    EXPECT_EQ(EValueType::Boolean, error.Attributes().Get<EValueType>("result_type"));

    builder.RegisterFunction(
        "is_null",
        MakeDescriptor({EValueType::Null}, callback, EValueType::Boolean, EOperationNullPolicy::PassToCallback));
    auto program = CompileExpression(expression, /*schema*/ {}, builder.Build());

    auto rowBuffer = New<TRowBuffer>();
    std::vector<TValue> scratch(program.GetScratchValueCount());
    auto result = MakeUnversionedNullValue();
    program.Evaluate(&result, /*inputRow*/ {}, scratch, rowBuffer);

    EXPECT_EQ(EValueType::Boolean, result.Type);
    EXPECT_TRUE(result.Data.Boolean);
    EXPECT_EQ(1, callCount);
}

TEST(TPortableExpressionProgramTest, NullPropagationDoesNotHideUnsupportedArguments)
{
    TExpressionRegistryBuilder builder;
    builder.RegisterFunction(
        "outer",
        MakeDescriptor(
            {EValueType::Null, EValueType::Int64},
            BIND(&CopyFirstArgument),
            EValueType::Int64,
            EOperationNullPolicy::Propagate));
    auto expression = MakeFunction(
        "outer",
        {
            MakeLiteral(MakeUnversionedNullValue()),
            MakeFunction("missing", /*arguments*/ {}),
        });
    auto error = GetCompilationError(expression, /*schema*/ {}, builder.Build());

    ASSERT_FALSE(error.IsOK());
    EXPECT_EQ("root.arguments[1]", error.Attributes().Get<std::string>("expression_path"));
    EXPECT_EQ("missing", error.Attributes().Get<std::string>("operation"));
}

TEST(TPortableExpressionProgramTest, ProgramOwnsLiteralsAndCallbacksAfterMove)
{
    const std::string expected(100, 'a');
    bool literalHolderShared = false;
    TWeakPtr<TSharedRangeHolder> literalHolder;
    std::weak_ptr<std::string> callbackState;
    auto program = [&] {
        auto literal = New<TLiteralExpression>(
            EValueType::String,
            TOwningValue(MakeUnversionedStringValue(expected)));
        const auto* literalData = static_cast<TValue>(literal->Value).Data.String;
        literalHolder = MakeWeak(literal->Value.GetStringHolder());
        auto ownedState = std::make_shared<std::string>(expected);
        callbackState = ownedState;
        TExpressionRegistryBuilder builder;
        builder.RegisterFunction(
            "check",
            MakeDescriptor(
                {EValueType::String, EValueType::Int64},
                BIND([&, ownedState, literalData] (
                    TValue* result,
                    TRange<TValue> arguments,
                    const TRowBufferPtr& /*rowBuffer*/) {
                    literalHolderShared = arguments[0].Data.String == literalData;
                    *result = MakeUnversionedBooleanValue(
                        arguments[0].AsString() == *ownedState && arguments[1].Data.Int64 == 42);
                }),
                EValueType::Boolean));
        auto registry = builder.Build();
        TTableSchema schema({TColumnSchema("x", EValueType::Int64)});
        auto expression = MakeFunction("check", {literal, MakeReference("x")}, EValueType::Boolean);
        return CompileExpression(expression, schema, registry);
    }();
    auto movedProgram = std::move(program);
    ASSERT_FALSE(literalHolder.IsExpired());
    ASSERT_FALSE(callbackState.expired());

    auto rowBuffer = New<TRowBuffer>();
    std::vector<TValue> scratch(movedProgram.GetScratchValueCount());
    std::array row{MakeUnversionedInt64Value(42)};
    auto result = MakeUnversionedNullValue();
    movedProgram.Evaluate(&result, row, scratch, rowBuffer);

    EXPECT_EQ(EValueType::Boolean, result.Type);
    EXPECT_TRUE(result.Data.Boolean);
    EXPECT_TRUE(literalHolderShared);
}

TEST(TPortableExpressionProgramTest, RootLiteralStringOutlivesProgram)
{
    auto rowBuffer = New<TRowBuffer>();
    auto result = MakeUnversionedNullValue(/*id*/ 5, EValueFlags::Aggregate);
    {
        std::string source(100, 'l');
        auto expression = New<TLiteralExpression>(
            EValueType::String,
            TOwningValue(MakeUnversionedStringValue(source)));
        const auto* literalData = static_cast<TValue>(expression->Value).Data.String;
        auto program = CompileExpression(expression, /*schema*/ {}, /*registry*/ {});
        std::vector<TValue> scratch(program.GetScratchValueCount());
        program.Evaluate(&result, /*inputRow*/ {}, scratch, rowBuffer);

        EXPECT_NE(literalData, result.Data.String);
        source.assign(source.size(), 'x');
    }

    EXPECT_EQ(EValueType::String, result.Type);
    EXPECT_EQ(std::string(100, 'l'), result.AsString());
    EXPECT_EQ(5, result.Id);
    EXPECT_EQ(EValueFlags::Aggregate, result.Flags);
}

TEST(TPortableExpressionProgramTest, RootReferenceStringOutlivesInput)
{
    auto rowBuffer = New<TRowBuffer>();
    auto result = MakeUnversionedNullValue();
    {
        TTableSchema schema({TColumnSchema("x", EValueType::String)});
        auto program = CompileExpression(MakeReference("x", EValueType::String), schema, /*registry*/ {});
        std::string source(100, 'r');
        std::array row{MakeUnversionedStringValue(source)};
        std::vector<TValue> scratch(program.GetScratchValueCount());
        program.Evaluate(&result, row, scratch, rowBuffer);

        EXPECT_NE(source.data(), result.Data.String);
        source.assign(source.size(), 'x');
    }

    EXPECT_EQ(EValueType::String, result.Type);
    EXPECT_EQ(std::string(100, 'r'), result.AsString());
}

TEST(TPortableExpressionProgramTest, RootCallbackStringIsNotCapturedTwice)
{
    auto rowBuffer = New<TRowBuffer>();
    auto result = MakeUnversionedNullValue();
    const char* capturedData = nullptr;
    i64 capturedSize = 0;
    {
        TExpressionRegistryBuilder builder;
        builder.RegisterFunction(
            "concat",
            MakeDescriptor(
                {EValueType::String, EValueType::String},
                BIND([&] (TValue* result, TRange<TValue> arguments, const TRowBufferPtr& rowBuffer) {
                    auto joined = arguments[0].AsString() + arguments[1].AsString();
                    *result = rowBuffer->CaptureValue(MakeUnversionedStringValue(joined));
                    capturedData = result->Data.String;
                    capturedSize = rowBuffer->GetSize();
                }),
                EValueType::String));
        auto expression = MakeFunction(
            "concat",
            {
                MakeReference("x", EValueType::String),
                MakeLiteral(MakeUnversionedStringValue("-suffix")),
            },
            EValueType::String);
        TTableSchema schema({TColumnSchema("x", EValueType::String)});
        auto program = CompileExpression(expression, schema, builder.Build());
        std::string source = "prefix";
        std::array row{MakeUnversionedStringValue(source)};
        std::vector<TValue> scratch(program.GetScratchValueCount());
        program.Evaluate(&result, row, scratch, rowBuffer);
        source.assign(source.size(), 'x');
    }

    EXPECT_EQ(EValueType::String, result.Type);
    EXPECT_EQ("prefix-suffix", result.AsString());
    EXPECT_EQ(capturedData, result.Data.String);
    EXPECT_EQ(capturedSize, rowBuffer->GetSize());
}

TEST(TPortableExpressionProgramTest, RootAnyLiteralAndReferenceOutliveSources)
{
    for (bool useReference : {false, true}) {
        SCOPED_TRACE(useReference);
        auto rowBuffer = New<TRowBuffer>();
        auto result = MakeUnversionedNullValue();
        {
            std::string source = "{key=[1;2;3;];}";
            std::array row{MakeUnversionedAnyValue(source)};
            TTableSchema schema({TColumnSchema("x", EValueType::Any)});
            auto expression = useReference ? MakeReference("x", EValueType::Any) : MakeLiteral(row[0]);
            const auto* sourceData = source.data();
            if (const auto* literal = expression->As<TLiteralExpression>()) {
                sourceData = static_cast<TValue>(literal->Value).Data.String;
            }
            auto program = CompileExpression(expression, schema, /*registry*/ {});
            std::vector<TValue> scratch(program.GetScratchValueCount());
            program.Evaluate(&result, row, scratch, rowBuffer);

            EXPECT_NE(sourceData, result.Data.String);
            source.assign(source.size(), 'x');
        }
        EXPECT_EQ(EValueType::Any, result.Type);
        EXPECT_EQ("{key=[1;2;3;];}", result.AsString());
    }
}

TEST(TPortableExpressionProgramTest, OutputCanAliasInput)
{
    TExpressionRegistryBuilder builder;
    builder.RegisterBinary(
        EBinaryOp::Minus,
        MakeDescriptor(
            {EValueType::Int64, EValueType::Int64},
            BIND([] (TValue* result, TRange<TValue> arguments, const TRowBufferPtr& /*rowBuffer*/) {
                *result = MakeUnversionedInt64Value(arguments[0].Data.Int64 - arguments[1].Data.Int64);
            })));
    auto expression = New<TBinaryOpExpression>(
        EValueType::Int64,
        EBinaryOp::Minus,
        MakeReference("y"),
        MakeReference("x"));
    TTableSchema schema({
        TColumnSchema("x", EValueType::Int64),
        TColumnSchema("y", EValueType::Int64),
    });
    auto program = CompileExpression(expression, schema, builder.Build());
    std::array row{
        MakeUnversionedInt64Value(5, /*id*/ 7, EValueFlags::Aggregate),
        MakeUnversionedInt64Value(20, /*id*/ 11),
    };
    auto rowBuffer = New<TRowBuffer>();
    std::vector<TValue> scratch(program.GetScratchValueCount());
    program.Evaluate(&row[0], row, scratch, rowBuffer);

    EXPECT_EQ(15, row[0].Data.Int64);
    EXPECT_EQ(7, row[0].Id);
    EXPECT_EQ(EValueFlags::Aggregate, row[0].Flags);
    EXPECT_EQ(20, row[1].Data.Int64);
}

TEST(TPortableExpressionProgramTest, ScratchCanBeReusedAfterCallbackException)
{
    TExpressionRegistryBuilder builder;
    builder.RegisterFunction(
        "check",
        MakeDescriptor(
            {EValueType::Int64},
            BIND([] (TValue* result, TRange<TValue> arguments, const TRowBufferPtr& /*rowBuffer*/) {
                if (arguments[0].Data.Int64 < 0) {
                    throw std::runtime_error("negative test argument");
                }
                *result = MakeUnversionedInt64Value(arguments[0].Data.Int64 + 1);
            })));
    TTableSchema schema({TColumnSchema("x", EValueType::Int64)});
    auto program = CompileExpression(
        MakeFunction("check", {MakeReference("x")}),
        schema,
        builder.Build());
    auto rowBuffer = New<TRowBuffer>();
    std::vector<TValue> scratch(program.GetScratchValueCount());
    std::array row{MakeUnversionedInt64Value(-1)};
    auto result = MakeUnversionedInt64Value(77);
    EXPECT_THROW_WITH_SUBSTRING(
        program.Evaluate(&result, row, scratch, rowBuffer),
        "negative test argument");
    EXPECT_EQ(77, result.Data.Int64);

    row[0] = MakeUnversionedInt64Value(41);
    program.Evaluate(&result, row, scratch, rowBuffer);
    EXPECT_EQ(42, result.Data.Int64);
}

TEST(TPortableExpressionProgramTest, LargeVariadicCallReusesCallerStorage)
{
    constexpr int ArgumentCount = 300;
    const TValue* expectedArguments = nullptr;
    TExpressionRegistryBuilder builder;
    builder.RegisterVariadicFunction(
        "sum",
        {
            .AllowedArgumentTypes = TTypeSet({EValueType::Int64}),
            .ResultType = EValueType::Int64,
            .Implementation = {
                .Callback = BIND([&] (
                    TValue* result,
                    TRange<TValue> arguments,
                    const TRowBufferPtr& /*rowBuffer*/) {
                    EXPECT_EQ(expectedArguments, arguments.begin());
                    EXPECT_EQ(ArgumentCount, arguments.size());

                    i64 sum = 0;
                    for (const auto& argument : arguments) {
                        sum += argument.Data.Int64;
                    }
                    *result = MakeUnversionedInt64Value(sum);
                }),
            },
        });
    std::vector<TConstExpressionPtr> arguments(ArgumentCount, MakeReference("x"));
    TTableSchema schema({TColumnSchema("x", EValueType::Int64)});
    auto program = CompileExpression(
        MakeFunction("sum", std::move(arguments)),
        schema,
        builder.Build());
    EXPECT_EQ(2 * ArgumentCount + 1, program.GetScratchValueCount());

    auto rowBuffer = New<TRowBuffer>();
    std::vector<TValue> scratch(program.GetScratchValueCount());
    expectedArguments = scratch.data() + ArgumentCount + 1;
    const i64 bufferSize = rowBuffer->GetSize();
    for (i64 value = 0; value < 10; ++value) {
        std::array row{MakeUnversionedInt64Value(value)};
        auto result = MakeUnversionedNullValue();
        program.Evaluate(&result, row, scratch, rowBuffer);

        EXPECT_EQ(ArgumentCount * value, result.Data.Int64);
        EXPECT_EQ(bufferSize, rowBuffer->GetSize());
    }
}

TEST(TPortableExpressionProgramTest, ConcurrentExecutionsUseIndependentScratch)
{
    constexpr int ThreadCount = 4;
    constexpr int IterationCount = 1'000;
    std::barrier synchronization(ThreadCount);
    TExpressionRegistryBuilder builder;
    builder.RegisterFunction(
        "increment",
        MakeDescriptor(
            {EValueType::Int64},
            BIND([&] (TValue* result, TRange<TValue> arguments, const TRowBufferPtr& /*rowBuffer*/) {
                synchronization.arrive_and_wait();
                i64 value = arguments[0].Data.Int64;
                synchronization.arrive_and_wait();
                *result = MakeUnversionedInt64Value(value + 1);
            })));
    TTableSchema schema({TColumnSchema("x", EValueType::Int64)});
    const auto program = CompileExpression(
        MakeFunction("increment", {MakeReference("x")}),
        schema,
        builder.Build());
    std::atomic<bool> successful = true;
    std::vector<std::thread> threads;
    for (int threadIndex = 0; threadIndex < ThreadCount; ++threadIndex) {
        threads.emplace_back([&, threadIndex] {
            try {
                auto rowBuffer = New<TRowBuffer>();
                std::vector<TValue> scratch(program.GetScratchValueCount());
                for (int iteration = 0; iteration < IterationCount; ++iteration) {
                    i64 value = threadIndex * IterationCount + iteration;
                    std::array row{MakeUnversionedInt64Value(value)};
                    auto result = MakeUnversionedNullValue();
                    program.Evaluate(&result, row, scratch, rowBuffer);
                    if (result.Type != EValueType::Int64 || result.Data.Int64 != value + 1) {
                        successful = false;
                    }
                }
            } catch (...) {
                successful = false;
                synchronization.arrive_and_drop();
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_TRUE(successful);
}

TEST(TPortableExpressionProgramTest, UnknownFunctionReportsNestedPathAndSignature)
{
    TExpressionRegistryBuilder builder;
    builder.RegisterFunction(
        "outer",
        MakeDescriptor({EValueType::Int64, EValueType::Int64}, BIND(&CopyFirstArgument)));
    builder.RegisterBinary(
        EBinaryOp::Plus,
        MakeDescriptor({EValueType::Int64, EValueType::Int64}, BIND(&CopyFirstArgument)));
    auto expression = MakeFunction(
        "outer",
        {
            MakeLiteral(MakeUnversionedInt64Value(1)),
            New<TBinaryOpExpression>(
                EValueType::Int64,
                EBinaryOp::Plus,
                MakeFunction("missing", {MakeLiteral(MakeUnversionedUint64Value(2))}),
                MakeLiteral(MakeUnversionedInt64Value(3))),
        });
    auto error = GetCompilationError(expression, /*schema*/ {}, builder.Build());
    ASSERT_FALSE(error.IsOK());
    EXPECT_EQ("root.arguments[1].lhs", error.Attributes().Get<std::string>("expression_path"));
    EXPECT_EQ("function", error.Attributes().Get<std::string>("expression_kind"));
    EXPECT_EQ("missing", error.Attributes().Get<std::string>("operation"));
    EXPECT_EQ(
        std::vector<EValueType>({EValueType::Uint64}),
        error.Attributes().Get<std::vector<EValueType>>("argument_types"));
    EXPECT_EQ(EValueType::Int64, error.Attributes().Get<EValueType>("result_type"));
}

TEST(TPortableExpressionProgramTest, RegisteredFunctionRejectsUnsupportedSignature)
{
    TExpressionRegistryBuilder builder;
    builder.RegisterFunction("identity", MakeDescriptor({EValueType::Int64}, BIND(&CopyFirstArgument)));
    auto registry = builder.Build();
    std::array expressions{
        MakeFunction("identity", {MakeLiteral(MakeUnversionedUint64Value(1))}),
        MakeFunction("identity", {MakeLiteral(MakeUnversionedInt64Value(1))}, EValueType::Uint64),
        MakeFunction("identity", /*arguments*/ {}),
    };
    for (const auto& expression : expressions) {
        auto error = GetCompilationError(expression, /*schema*/ {}, registry);
        ASSERT_FALSE(error.IsOK());
        EXPECT_EQ("root", error.Attributes().Get<std::string>("expression_path"));
        EXPECT_EQ("identity", error.Attributes().Get<std::string>("operation"));
        EXPECT_EQ(expression->GetWireType(), error.Attributes().Get<EValueType>("result_type"));
    }
}

TEST(TPortableExpressionProgramTest, UnknownOperatorsReportSignature)
{
    auto literal = MakeLiteral(MakeUnversionedInt64Value(1));
    auto unary = New<TUnaryOpExpression>(EValueType::Int64, EUnaryOp::Minus, literal);
    auto unaryError = GetCompilationError(unary, /*schema*/ {}, /*registry*/ {});
    ASSERT_FALSE(unaryError.IsOK());
    EXPECT_EQ("root", unaryError.Attributes().Get<std::string>("expression_path"));
    EXPECT_EQ("unary_op", unaryError.Attributes().Get<std::string>("expression_kind"));
    EXPECT_EQ(Format("%lv", EUnaryOp::Minus), unaryError.Attributes().Get<std::string>("operation"));
    EXPECT_EQ(
        std::vector<EValueType>({EValueType::Int64}),
        unaryError.Attributes().Get<std::vector<EValueType>>("argument_types"));

    auto binary = New<TBinaryOpExpression>(EValueType::Int64, EBinaryOp::Minus, literal, literal);
    auto binaryError = GetCompilationError(binary, /*schema*/ {}, /*registry*/ {});
    ASSERT_FALSE(binaryError.IsOK());
    EXPECT_EQ("binary_op", binaryError.Attributes().Get<std::string>("expression_kind"));
    EXPECT_EQ(Format("%lv", EBinaryOp::Minus), binaryError.Attributes().Get<std::string>("operation"));
    EXPECT_EQ(
        std::vector<EValueType>({EValueType::Int64, EValueType::Int64}),
        binaryError.Attributes().Get<std::vector<EValueType>>("argument_types"));
    EXPECT_EQ(EValueType::Int64, binaryError.Attributes().Get<EValueType>("result_type"));
}

TEST(TPortableExpressionProgramTest, MissingColumnReportsOperandPath)
{
    TExpressionRegistryBuilder builder;
    builder.RegisterUnary(EUnaryOp::Minus, MakeDescriptor({EValueType::Int64}, BIND(&CopyFirstArgument)));
    auto expression = New<TUnaryOpExpression>(
        EValueType::Int64,
        EUnaryOp::Minus,
        MakeReference("missing"));
    auto error = GetCompilationError(expression, /*schema*/ {}, builder.Build());
    ASSERT_FALSE(error.IsOK());
    EXPECT_EQ("root.operand", error.Attributes().Get<std::string>("expression_path"));
    EXPECT_EQ("reference", error.Attributes().Get<std::string>("expression_kind"));
    EXPECT_NE(std::string::npos, ToString(error).find("missing"));
}

TEST(TPortableExpressionProgramTest, RejectsEveryKnownUnsupportedExpressionKind)
{
    std::vector<std::pair<std::string, TConstExpressionPtr>> expressions{
        {"in", New<TInExpression>(EValueType::Boolean)},
        {"between", New<TBetweenExpression>(EValueType::Boolean)},
        {"transform", New<TTransformExpression>(EValueType::Int64)},
        {"case", New<TCaseExpression>(EValueType::Int64)},
        {"like", New<TLikeExpression>(EValueType::Boolean)},
        {"composite_member_accessor", New<TCompositeMemberAccessorExpression>(
            SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"subquery", New<TSubqueryExpression>(EValueType::Int64)},
    };
    for (const auto& [kind, expression] : expressions) {
        SCOPED_TRACE(kind);
        auto error = GetCompilationError(expression, /*schema*/ {}, /*registry*/ {});
        ASSERT_FALSE(error.IsOK());
        EXPECT_EQ("root", error.Attributes().Get<std::string>("expression_path"));
        EXPECT_EQ(kind, error.Attributes().Get<std::string>("expression_kind"));
    }
}

TEST(TPortableExpressionProgramTest, RejectsUnregisteredLazyFunctions)
{
    std::vector<std::pair<std::string, std::vector<TConstExpressionPtr>>> functions{
        {"if", {
            MakeLiteral(MakeUnversionedBooleanValue(true)),
            MakeLiteral(MakeUnversionedInt64Value(1)),
            MakeLiteral(MakeUnversionedInt64Value(2)),
        }},
        {"coalesce", {
            MakeLiteral(MakeUnversionedInt64Value(1)),
            MakeLiteral(MakeUnversionedInt64Value(2)),
        }},
    };
    for (const auto& [functionName, arguments] : functions) {
        SCOPED_TRACE(functionName);
        auto expression = MakeFunction(functionName, arguments);
        auto error = GetCompilationError(expression, /*schema*/ {}, /*registry*/ {});
        ASSERT_FALSE(error.IsOK());
        EXPECT_EQ("root", error.Attributes().Get<std::string>("expression_path"));
        EXPECT_EQ("function", error.Attributes().Get<std::string>("expression_kind"));
        EXPECT_EQ(functionName, error.Attributes().Get<std::string>("operation"));
    }
}

TEST(TPortableExpressionProgramDeathTest, RejectsInsufficientScratch)
{
    auto program = CompileExpression(MakeLiteral(MakeUnversionedInt64Value(1)), /*schema*/ {}, /*registry*/ {});
    auto rowBuffer = New<TRowBuffer>();
    auto result = MakeUnversionedNullValue();
    EXPECT_DEATH(program.Evaluate(&result, /*inputRow*/ {}, /*scratch*/ {}, rowBuffer), "scratch");
}

TEST(TPortableExpressionProgramDeathTest, RejectsInsufficientInputRow)
{
    TTableSchema schema({TColumnSchema("x", EValueType::Int64)});
    auto program = CompileExpression(MakeReference("x"), schema, /*registry*/ {});
    auto rowBuffer = New<TRowBuffer>();
    std::vector<TValue> scratch(program.GetScratchValueCount());
    auto result = MakeUnversionedNullValue();
    EXPECT_DEATH(program.Evaluate(&result, /*inputRow*/ {}, scratch, rowBuffer), "inputRow");
}

TEST(TPortableExpressionProgramDeathTest, RejectsMovedFromProgram)
{
    auto program = CompileExpression(
        MakeLiteral(MakeUnversionedInt64Value(42)),
        /*schema*/ {},
        /*registry*/ {});
    auto movedProgram = std::move(program);

    auto rowBuffer = New<TRowBuffer>();
    std::vector<TValue> scratch(movedProgram.GetScratchValueCount());
    auto result = MakeUnversionedNullValue();
    movedProgram.Evaluate(&result, /*inputRow*/ {}, scratch, rowBuffer);
    EXPECT_EQ(EValueType::Int64, result.Type);
    EXPECT_EQ(42, result.Data.Int64);

    EXPECT_DEATH(program.Evaluate(&result, /*inputRow*/ {}, scratch, rowBuffer), "!Nodes_\\.empty\\(\\)");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient::NPortable
