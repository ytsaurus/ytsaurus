#include <yt/yt/library/query/portable_engine/registry.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/test_framework/framework.h>

#include <array>
#include <atomic>
#include <initializer_list>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace NYT::NQueryClient::NPortable {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void IgnoreOperation(
    TValue* /*result*/,
    TRange<TValue> /*arguments*/,
    const TRowBufferPtr& /*rowBuffer*/)
{ }

TOperationDescriptor MakeDescriptor(
    std::initializer_list<EValueType> argumentTypes,
    EValueType resultType = EValueType::Int64)
{
    return {
        .ArgumentTypes = std::vector<EValueType>(argumentTypes),
        .ResultType = resultType,
        .Implementation = {
            .Callback = BIND(&IgnoreOperation),
        },
    };
}

TVariadicOperationDescriptor MakeVariadicDescriptor(
    std::initializer_list<EValueType> allowedArgumentTypes,
    EValueType resultType = EValueType::Int64)
{
    return {
        .AllowedArgumentTypes = TTypeSet(allowedArgumentTypes),
        .ResultType = resultType,
        .Implementation = {
            .Callback = BIND(&IgnoreOperation),
        },
    };
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPortableExpressionRegistryTest, ExactLookupUsesKindOpcodeAndResultType)
{
    TExpressionRegistryBuilder builder;
    builder.RegisterFunction("identity", MakeDescriptor({EValueType::Int64}));
    auto uintIdentity = MakeDescriptor({EValueType::Int64}, EValueType::Uint64);
    uintIdentity.Implementation.NullPolicy = EOperationNullPolicy::PassToCallback;
    builder.RegisterFunction("identity", std::move(uintIdentity));
    builder.RegisterUnary(EUnaryOp::Minus, MakeDescriptor({EValueType::Int64}));
    builder.RegisterBinary(
        EBinaryOp::Plus,
        MakeDescriptor({EValueType::Int64, EValueType::Int64}));

    auto registry = builder.Build();

    auto intFunction = registry.FindFunction(
        "identity",
        {EValueType::Int64},
        EValueType::Int64);
    ASSERT_TRUE(intFunction);
    EXPECT_EQ(EOperationNullPolicy::Propagate, intFunction->Implementation.NullPolicy);
    EXPECT_EQ(1, intFunction->ArgumentCount);

    auto uintFunction = registry.FindFunction(
        "identity",
        {EValueType::Int64},
        EValueType::Uint64);
    ASSERT_TRUE(uintFunction);
    EXPECT_EQ(EOperationNullPolicy::PassToCallback, uintFunction->Implementation.NullPolicy);
    EXPECT_EQ(1, uintFunction->ArgumentCount);

    EXPECT_FALSE(registry.FindFunction(
        "identity",
        {EValueType::Uint64},
        EValueType::Int64));
    EXPECT_FALSE(registry.FindFunction(
        "missing",
        {EValueType::Int64},
        EValueType::Int64));
    EXPECT_FALSE(registry.FindFunction(
        "identity",
        {EValueType::Max},
        EValueType::Int64));
    EXPECT_FALSE(registry.FindFunction(
        "identity",
        {EValueType::Int64},
        EValueType::TheBottom));
    EXPECT_FALSE(registry.FindFunction(
        "identity",
        {},
        EValueType::Int64));
    EXPECT_FALSE(registry.FindFunction(
        "identity",
        {EValueType::Int64, EValueType::Int64},
        EValueType::Int64));

    auto unary = registry.FindUnary(
        EUnaryOp::Minus,
        EValueType::Int64,
        EValueType::Int64);
    ASSERT_TRUE(unary);
    EXPECT_EQ(1, unary->ArgumentCount);
    EXPECT_FALSE(registry.FindUnary(
        EUnaryOp::Plus,
        EValueType::Int64,
        EValueType::Int64));
    EXPECT_FALSE(registry.FindUnary(
        static_cast<EUnaryOp>(-1),
        EValueType::Int64,
        EValueType::Int64));
    EXPECT_FALSE(registry.FindUnary(
        EUnaryOp::Minus,
        EValueType::Max,
        EValueType::Int64));

    auto binary = registry.FindBinary(
        EBinaryOp::Plus,
        EValueType::Int64,
        EValueType::Int64,
        EValueType::Int64);
    ASSERT_TRUE(binary);
    EXPECT_EQ(2, binary->ArgumentCount);
    EXPECT_FALSE(registry.FindBinary(
        EBinaryOp::Minus,
        EValueType::Int64,
        EValueType::Int64,
        EValueType::Int64));
    EXPECT_FALSE(registry.FindBinary(
        static_cast<EBinaryOp>(-1),
        EValueType::Int64,
        EValueType::Int64,
        EValueType::Int64));
    EXPECT_FALSE(registry.FindBinary(
        EBinaryOp::Plus,
        EValueType::Int64,
        EValueType::Int64,
        EValueType::Max));
}

TEST(TPortableExpressionRegistryTest, VariadicFunctionAcceptsOneOrMoreAllowedArguments)
{
    TExpressionRegistryBuilder builder;
    builder.RegisterVariadicFunction(
        "variadic",
        MakeVariadicDescriptor(
            {EValueType::Int64, EValueType::Uint64},
            EValueType::Uint64));

    auto registry = builder.Build();

    auto singleArgument = registry.FindFunction(
        "variadic",
        {EValueType::Int64},
        EValueType::Uint64);
    ASSERT_TRUE(singleArgument);
    EXPECT_EQ(1, singleArgument->ArgumentCount);

    auto threeArguments = registry.FindFunction(
        "variadic",
        {EValueType::Uint64, EValueType::Int64, EValueType::Uint64},
        EValueType::Uint64);
    ASSERT_TRUE(threeArguments);
    EXPECT_EQ(3, threeArguments->ArgumentCount);
    EXPECT_FALSE(registry.FindFunction(
        "variadic",
        {},
        EValueType::Uint64));
    EXPECT_FALSE(registry.FindFunction(
        "variadic",
        {EValueType::Double},
        EValueType::Uint64));
    EXPECT_FALSE(registry.FindFunction(
        "variadic",
        {EValueType::Int64},
        EValueType::Int64));
    EXPECT_FALSE(registry.FindFunction(
        "variadic",
        {EValueType::Max},
        EValueType::Uint64));
}

TEST(TPortableExpressionRegistryTest, BuildCreatesIndependentSnapshot)
{
    TExpressionRegistryBuilder builder;
    builder.RegisterFunction("first", MakeDescriptor({}));
    auto firstRegistry = builder.Build();

    builder.RegisterFunction("first", MakeDescriptor({EValueType::String}));
    builder.RegisterFunction("second", MakeDescriptor({}));
    builder.RegisterVariadicFunction(
        "variadic",
        MakeVariadicDescriptor({EValueType::Int64}));
    auto secondRegistry = builder.Build();

    EXPECT_TRUE(firstRegistry.FindFunction("first", {}, EValueType::Int64));
    EXPECT_FALSE(firstRegistry.FindFunction(
        "first",
        {EValueType::String},
        EValueType::Int64));
    EXPECT_FALSE(firstRegistry.FindFunction("second", {}, EValueType::Int64));
    EXPECT_FALSE(firstRegistry.FindFunction(
        "variadic",
        {EValueType::Int64},
        EValueType::Int64));
    EXPECT_TRUE(secondRegistry.FindFunction("first", {}, EValueType::Int64));
    EXPECT_TRUE(secondRegistry.FindFunction(
        "first",
        {EValueType::String},
        EValueType::Int64));
    EXPECT_TRUE(secondRegistry.FindFunction("second", {}, EValueType::Int64));
    EXPECT_TRUE(secondRegistry.FindFunction(
        "variadic",
        {EValueType::Int64},
        EValueType::Int64));
}

TEST(TPortableExpressionRegistryTest, ResolvedOperationOutlivesRegistry)
{
    std::optional<TResolvedOperation> operation;
    {
        auto descriptor = MakeDescriptor({EValueType::Int64});
        descriptor.Implementation.Callback = BIND([] (
            TValue* result,
            TRange<TValue> arguments,
            const TRowBufferPtr& /*rowBuffer*/) {
            *result = MakeUnversionedInt64Value(arguments[0].Data.Int64 + 1);
        });

        TExpressionRegistryBuilder builder;
        builder.RegisterFunction("increment", std::move(descriptor));
        auto registry = builder.Build();
        operation = registry.FindFunction(
            "increment",
            {EValueType::Int64},
            EValueType::Int64);
    }

    ASSERT_TRUE(operation);
    auto rowBuffer = New<TRowBuffer>();
    std::array arguments{MakeUnversionedInt64Value(41)};
    TValue result = MakeUnversionedNullValue();
    InvokeOperation(*operation, &result, arguments, rowBuffer);

    EXPECT_EQ(EValueType::Int64, result.Type);
    EXPECT_EQ(42, result.Data.Int64);
}

TEST(TPortableExpressionRegistryTest, RejectsInvalidDescriptors)
{
    TExpressionRegistryBuilder builder;

    auto emptyCallback = MakeDescriptor({});
    emptyCallback.Implementation.Callback = {};
    EXPECT_THROW_WITH_SUBSTRING(
        builder.RegisterFunction("empty_callback", emptyCallback),
        "empty callback");

    EXPECT_THROW_WITH_SUBSTRING(
        builder.RegisterFunction(
            "invalid_result",
            MakeDescriptor({}, EValueType::TheBottom)),
        "invalid result type");
    EXPECT_THROW_WITH_SUBSTRING(
        builder.RegisterFunction(
            "invalid_argument",
            MakeDescriptor({EValueType::TheBottom})),
        "invalid type");

    auto emptyVariadicCallback = MakeVariadicDescriptor({EValueType::Int64});
    emptyVariadicCallback.Implementation.Callback = {};
    EXPECT_THROW_WITH_SUBSTRING(
        builder.RegisterVariadicFunction(
            "empty_variadic_callback",
            emptyVariadicCallback),
        "empty callback");
    EXPECT_THROW_WITH_SUBSTRING(
        builder.RegisterVariadicFunction(
            "invalid_variadic_result",
            MakeVariadicDescriptor(
                {EValueType::Int64},
                EValueType::TheBottom)),
        "invalid result type");
    EXPECT_THROW_WITH_SUBSTRING(
        builder.RegisterVariadicFunction(
            "empty_allowed_types",
            MakeVariadicDescriptor({})),
        "empty allowed argument type set");
    EXPECT_THROW_WITH_SUBSTRING(
        builder.RegisterVariadicFunction(
            "invalid_allowed_type",
            MakeVariadicDescriptor({EValueType::TheBottom})),
        "invalid allowed argument type set");
}

TEST(TPortableExpressionRegistryTest, RejectsInvalidNamesOpcodesAndArity)
{
    TExpressionRegistryBuilder builder;

    EXPECT_THROW_WITH_SUBSTRING(
        builder.RegisterFunction("", MakeDescriptor({})),
        "name cannot be empty");
    EXPECT_THROW_WITH_SUBSTRING(
        builder.RegisterVariadicFunction(
            "",
            MakeVariadicDescriptor({EValueType::Int64})),
        "name cannot be empty");
    EXPECT_THROW_WITH_SUBSTRING(
        builder.RegisterUnary(
            static_cast<EUnaryOp>(-1),
            MakeDescriptor({EValueType::Int64})),
        "invalid opcode");
    EXPECT_THROW_WITH_SUBSTRING(
        builder.RegisterBinary(
            static_cast<EBinaryOp>(-1),
            MakeDescriptor({EValueType::Int64, EValueType::Int64})),
        "invalid opcode");
    EXPECT_THROW_WITH_SUBSTRING(
        builder.RegisterUnary(EUnaryOp::Minus, MakeDescriptor({})),
        "arity 1");
    EXPECT_THROW_WITH_SUBSTRING(
        builder.RegisterBinary(
            EBinaryOp::Plus,
            MakeDescriptor({EValueType::Int64})),
        "arity 2");
}

TEST(TPortableExpressionRegistryTest, RejectsMixingExactAndVariadicFunctions)
{
    TExpressionRegistryBuilder exactFirstBuilder;
    exactFirstBuilder.RegisterFunction(
        "exact_first",
        MakeDescriptor({EValueType::Int64}));
    EXPECT_THROW_WITH_SUBSTRING(
        exactFirstBuilder.RegisterVariadicFunction(
            "exact_first",
            MakeVariadicDescriptor({EValueType::Int64})),
        "cannot mix exact and variadic overloads");

    TExpressionRegistryBuilder variadicFirstBuilder;
    variadicFirstBuilder.RegisterVariadicFunction(
        "variadic_first",
        MakeVariadicDescriptor({EValueType::Int64}));
    EXPECT_THROW_WITH_SUBSTRING(
        variadicFirstBuilder.RegisterFunction(
            "variadic_first",
            MakeDescriptor({EValueType::Int64})),
        "cannot mix exact and variadic overloads");

    EXPECT_THROW_WITH_SUBSTRING(
        variadicFirstBuilder.RegisterVariadicFunction(
            "variadic_first",
            MakeVariadicDescriptor(
                {EValueType::Uint64},
                EValueType::Uint64)),
        "variadic overload is already registered");
}

TEST(TPortableExpressionRegistryTest, FailedRegistrationDoesNotPoisonBuilder)
{
    TExpressionRegistryBuilder builder;
    builder.RegisterFunction("stable", MakeDescriptor({EValueType::Int64}));

    auto duplicate = MakeDescriptor({EValueType::Int64});
    duplicate.Implementation.NullPolicy = EOperationNullPolicy::PassToCallback;
    EXPECT_THROW_WITH_SUBSTRING(
        builder.RegisterFunction("stable", std::move(duplicate)),
        "already registered");
    EXPECT_THROW_WITH_SUBSTRING(
        builder.RegisterVariadicFunction(
            "stable",
            MakeVariadicDescriptor({EValueType::Int64})),
        "cannot mix exact and variadic overloads");

    builder.RegisterFunction(
        "stable",
        MakeDescriptor({EValueType::Int64}, EValueType::Uint64));
    builder.RegisterFunction("other", MakeDescriptor({EValueType::String}));
    auto registry = builder.Build();

    EXPECT_TRUE(registry.FindFunction(
        "stable",
        {EValueType::Int64},
        EValueType::Int64));
    EXPECT_TRUE(registry.FindFunction(
        "stable",
        {EValueType::Int64},
        EValueType::Uint64));
    EXPECT_TRUE(registry.FindFunction(
        "other",
        {EValueType::String},
        EValueType::Int64));
}

TEST(TPortableExpressionRegistryTest, PropagatesNullWithoutCallingCallback)
{
    int callCount = 0;
    auto descriptor = MakeDescriptor({EValueType::Int64});
    descriptor.Implementation.NullPolicy = EOperationNullPolicy::Propagate;
    descriptor.Implementation.Callback = BIND([&] (
        TValue* result,
        TRange<TValue> /*arguments*/,
        const TRowBufferPtr& /*rowBuffer*/) {
        ++callCount;
        *result = MakeUnversionedInt64Value(17);
    });

    TExpressionRegistryBuilder builder;
    builder.RegisterFunction("nullable", std::move(descriptor));
    auto registry = builder.Build();
    auto operation = registry.FindFunction(
        "nullable",
        {EValueType::Int64},
        EValueType::Int64);
    ASSERT_TRUE(operation);

    auto rowBuffer = New<TRowBuffer>();
    TValue result = MakeUnversionedInt64Value(42);
    std::array arguments{MakeUnversionedNullValue()};
    InvokeOperation(*operation, &result, arguments, rowBuffer);

    EXPECT_EQ(EValueType::Null, result.Type);
    EXPECT_EQ(0, callCount);

    arguments[0] = MakeUnversionedInt64Value(1);
    InvokeOperation(*operation, &result, arguments, rowBuffer);

    EXPECT_EQ(EValueType::Int64, result.Type);
    EXPECT_EQ(17, result.Data.Int64);
    EXPECT_EQ(1, callCount);
}

TEST(TPortableExpressionRegistryTest, PassesNullToCallback)
{
    int callCount = 0;
    auto descriptor = MakeDescriptor({EValueType::Int64}, EValueType::Boolean);
    descriptor.Implementation.NullPolicy = EOperationNullPolicy::PassToCallback;
    descriptor.Implementation.Callback = BIND([&] (
        TValue* result,
        TRange<TValue> arguments,
        const TRowBufferPtr& /*rowBuffer*/) {
        ++callCount;
        *result = MakeUnversionedBooleanValue(arguments[0].Type == EValueType::Null);
    });

    TExpressionRegistryBuilder builder;
    builder.RegisterFunction("is_null", std::move(descriptor));
    auto registry = builder.Build();
    auto operation = registry.FindFunction(
        "is_null",
        {EValueType::Int64},
        EValueType::Boolean);
    ASSERT_TRUE(operation);

    auto rowBuffer = New<TRowBuffer>();
    TValue result = MakeUnversionedNullValue();
    std::array arguments{MakeUnversionedNullValue()};
    InvokeOperation(*operation, &result, arguments, rowBuffer);

    EXPECT_EQ(EValueType::Boolean, result.Type);
    EXPECT_TRUE(result.Data.Boolean);
    EXPECT_EQ(1, callCount);
}

TEST(TPortableExpressionRegistryTest, CallbackCanCaptureStringResultInRowBuffer)
{
    auto descriptor = MakeDescriptor(
        {EValueType::String, EValueType::String},
        EValueType::String);
    bool captured = false;
    descriptor.Implementation.Callback = BIND([&] (
        TValue* result,
        TRange<TValue> arguments,
        const TRowBufferPtr& rowBuffer) {
        auto joined = arguments[0].AsString();
        joined += arguments[1].AsString();
        *result = MakeUnversionedStringValue(joined);
        rowBuffer->CaptureValue(result);
        captured = result->Data.String != joined.data();
    });

    TExpressionRegistryBuilder builder;
    builder.RegisterFunction("concat", std::move(descriptor));
    auto registry = builder.Build();
    auto operation = registry.FindFunction(
        "concat",
        {EValueType::String, EValueType::String},
        EValueType::String);
    ASSERT_TRUE(operation);

    auto rowBuffer = New<TRowBuffer>();
    TValue result = MakeUnversionedNullValue();
    std::string lhs = "left-";
    std::string rhs = "right";
    std::array arguments{
        MakeUnversionedStringValue(lhs),
        MakeUnversionedStringValue(rhs),
    };

    InvokeOperation(*operation, &result, arguments, rowBuffer);
    EXPECT_TRUE(captured);
    lhs.assign(lhs.size(), 'x');
    rhs.assign(rhs.size(), 'y');

    EXPECT_EQ(EValueType::String, result.Type);
    EXPECT_EQ("left-right", result.AsString());
}

TEST(TPortableExpressionRegistryTest, ImmutableRegistrySupportsConcurrentLookupAndInvocation)
{
    auto descriptor = MakeDescriptor({EValueType::Int64});
    descriptor.Implementation.Callback = BIND([] (
        TValue* result,
        TRange<TValue> arguments,
        const TRowBufferPtr& /*rowBuffer*/) {
        *result = MakeUnversionedInt64Value(arguments[0].Data.Int64 + 1);
    });

    TExpressionRegistryBuilder builder;
    builder.RegisterFunction("increment", std::move(descriptor));
    auto registry = builder.Build();

    constexpr int ThreadCount = 8;
    constexpr int IterationCount = 1'000;
    std::atomic<bool> successful = true;
    std::vector<std::thread> threads;
    threads.reserve(ThreadCount);

    for (int threadIndex = 0; threadIndex < ThreadCount; ++threadIndex) {
        threads.emplace_back([&registry, &successful, threadIndex] {
            try {
                auto rowBuffer = New<TRowBuffer>();
                for (int iteration = 0; iteration < IterationCount; ++iteration) {
                    auto operation = registry.FindFunction(
                        "increment",
                        {EValueType::Int64},
                        EValueType::Int64);
                    if (!operation) {
                        successful = false;
                        return;
                    }

                    const i64 argument = threadIndex * IterationCount + iteration;
                    std::array arguments{MakeUnversionedInt64Value(argument)};
                    TValue result = MakeUnversionedNullValue();
                    InvokeOperation(
                        *operation,
                        &result,
                        arguments,
                        rowBuffer);
                    if (result.Type != EValueType::Int64 ||
                        result.Data.Int64 != argument + 1)
                    {
                        successful = false;
                        return;
                    }
                }
            } catch (...) {
                successful = false;
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_TRUE(successful);
}

TEST(TPortableExpressionRegistryDeathTest, RejectsMismatchedArgumentCount)
{
    TExpressionRegistryBuilder builder;
    builder.RegisterFunction("one_argument", MakeDescriptor({EValueType::Int64}));
    auto registry = builder.Build();
    auto operation = registry.FindFunction(
        "one_argument",
        {EValueType::Int64},
        EValueType::Int64);
    ASSERT_TRUE(operation);

    auto rowBuffer = New<TRowBuffer>();
    TValue result = MakeUnversionedNullValue();
    EXPECT_DEATH(
        InvokeOperation(*operation, &result, {}, rowBuffer),
        "ArgumentCount");
}

TEST(TPortableExpressionRegistryTest, CallbackExceptionPropagates)
{
    int callCount = 0;
    auto descriptor = MakeDescriptor({});
    descriptor.Implementation.Callback = BIND([&] (
        TValue* /*result*/,
        TRange<TValue> /*arguments*/,
        const TRowBufferPtr& /*rowBuffer*/) {
        ++callCount;
        throw std::runtime_error("portable callback failed");
    });

    TExpressionRegistryBuilder builder;
    builder.RegisterFunction("fail", std::move(descriptor));
    auto registry = builder.Build();
    auto operation = registry.FindFunction("fail", {}, EValueType::Int64);
    ASSERT_TRUE(operation);

    auto rowBuffer = New<TRowBuffer>();
    TValue result = MakeUnversionedNullValue();
    EXPECT_THROW_WITH_SUBSTRING(
        InvokeOperation(*operation, &result, {}, rowBuffer),
        "portable callback failed");
    EXPECT_EQ(1, callCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient::NPortable
