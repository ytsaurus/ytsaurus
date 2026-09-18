#include "registry.h"

#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/error.h>

#include <algorithm>
#include <array>
#include <iterator>
#include <utility>

namespace NYT::NQueryClient::NPortable {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

using TOperationOverloads = std::vector<TOperationDescriptor>;

bool IsValidTypeSet(const TTypeSet& types)
{
    bool valid = true;
    types.ForEach([&] (EValueType type) {
        valid = valid && IsAnyColumnCompatibleType(type);
    });
    return valid;
}

void ValidateCommonDescriptorFields(
    const TOperationImplementation& implementation,
    EValueType resultType,
    const std::string& operation)
{
    if (!implementation.Callback) {
        THROW_ERROR_EXCEPTION("Portable %v has an empty callback", operation);
    }

    if (!IsAnyColumnCompatibleType(resultType)) {
        THROW_ERROR_EXCEPTION("Portable %v has invalid result type %Qlv",
            operation,
            resultType);
    }
}

void ValidateDescriptor(
    const TOperationDescriptor& descriptor,
    const std::string& operation,
    std::optional<int> expectedArity)
{
    ValidateCommonDescriptorFields(descriptor.Implementation, descriptor.ResultType, operation);

    for (ssize_t index = 0; index < std::ssize(descriptor.ArgumentTypes); ++index) {
        auto type = descriptor.ArgumentTypes[index];
        if (!IsAnyColumnCompatibleType(type)) {
            THROW_ERROR_EXCEPTION("Portable %v argument %v has invalid type %Qlv",
                operation,
                index,
                type);
        }
    }

    if (expectedArity && std::ssize(descriptor.ArgumentTypes) != *expectedArity) {
        THROW_ERROR_EXCEPTION("Portable %v must have arity %v",
            operation,
            *expectedArity);
    }
}

void ValidateVariadicDescriptor(
    const TVariadicOperationDescriptor& descriptor,
    const std::string& operation)
{
    ValidateCommonDescriptorFields(descriptor.Implementation, descriptor.ResultType, operation);

    if (descriptor.AllowedArgumentTypes.IsEmpty()) {
        THROW_ERROR_EXCEPTION("Portable %v has an empty allowed argument type set",
            operation);
    }

    if (!IsValidTypeSet(descriptor.AllowedArgumentTypes)) {
        THROW_ERROR_EXCEPTION("Portable %v has an invalid allowed argument type set",
            operation);
    }
}

bool MatchesExactSignature(
    const TOperationDescriptor& descriptor,
    TRange<EValueType> argumentTypes,
    EValueType resultType)
{
    return descriptor.ResultType == resultType &&
        std::ranges::equal(descriptor.ArgumentTypes, argumentTypes);
}

bool MatchesVariadicSignature(
    const TVariadicOperationDescriptor& descriptor,
    TRange<EValueType> argumentTypes,
    EValueType resultType)
{
    if (argumentTypes.empty() || descriptor.ResultType != resultType) {
        return false;
    }

    return std::all_of(argumentTypes.begin(), argumentTypes.end(), [&] (EValueType type) {
        return IsAnyColumnCompatibleType(type) &&
            descriptor.AllowedArgumentTypes.Get(type);
    });
}

bool HaveSameSignature(
    const TOperationDescriptor& lhs,
    const TOperationDescriptor& rhs)
{
    return lhs.ArgumentTypes == rhs.ArgumentTypes &&
        lhs.ResultType == rhs.ResultType;
}

void ValidateAndAppendDescriptor(
    TOperationOverloads* overloads,
    TOperationDescriptor descriptor,
    const std::string& operation)
{
    for (const auto& existingDescriptor : *overloads) {
        if (HaveSameSignature(existingDescriptor, descriptor)) {
            THROW_ERROR_EXCEPTION("Portable %v with this signature is already registered",
                operation);
        }
    }

    overloads->push_back(std::move(descriptor));
}

std::optional<TResolvedOperation> FindExactOperation(
    const TOperationOverloads& overloads,
    TRange<EValueType> argumentTypes,
    EValueType resultType)
{
    for (const auto& descriptor : overloads) {
        if (MatchesExactSignature(descriptor, argumentTypes, resultType)) {
            return TResolvedOperation{
                .Implementation = descriptor.Implementation,
                .ArgumentCount = std::ssize(argumentTypes),
            };
        }
    }

    return std::nullopt;
}

std::optional<TResolvedOperation> FindVariadicOperation(
    const TVariadicOperationDescriptor& descriptor,
    TRange<EValueType> argumentTypes,
    EValueType resultType)
{
    if (!MatchesVariadicSignature(descriptor, argumentTypes, resultType)) {
        return std::nullopt;
    }

    return TResolvedOperation{
        .Implementation = descriptor.Implementation,
        .ArgumentCount = std::ssize(argumentTypes),
    };
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void InvokeOperation(
    const TResolvedOperation& operation,
    TValue* result,
    TRange<TValue> arguments,
    const TRowBufferPtr& rowBuffer)
{
    YT_VERIFY(result);
    YT_VERIFY(std::ssize(arguments) == operation.ArgumentCount);

    const auto& implementation = operation.Implementation;
    YT_VERIFY(implementation.Callback);

    switch (implementation.NullPolicy) {
        case EOperationNullPolicy::Propagate:
            if (std::any_of(arguments.begin(), arguments.end(), [] (const TValue& argument) {
                return argument.Type == EValueType::Null;
            })) {
                *result = MakeUnversionedNullValue();
                return;
            }
            break;

        case EOperationNullPolicy::PassToCallback:
            break;

        default:
            YT_ABORT();
    }

    implementation.Callback(result, arguments, rowBuffer);
}

////////////////////////////////////////////////////////////////////////////////

std::optional<TResolvedOperation> TExpressionRegistry::FindFunction(
    const std::string& functionName,
    TRange<EValueType> argumentTypes,
    EValueType resultType) const
{
    auto functionIt = Functions_.find(functionName);
    if (functionIt != Functions_.end()) {
        return FindExactOperation(functionIt->second, argumentTypes, resultType);
    }

    auto variadicFunctionIt = VariadicFunctions_.find(functionName);
    return variadicFunctionIt == VariadicFunctions_.end()
        ? std::nullopt
        : FindVariadicOperation(variadicFunctionIt->second, argumentTypes, resultType);
}

std::optional<TResolvedOperation> TExpressionRegistry::FindUnary(
    EUnaryOp opcode,
    EValueType argumentType,
    EValueType resultType) const
{
    if (!TEnumTraits<EUnaryOp>::IsKnownValue(opcode)) {
        return std::nullopt;
    }

    std::array argumentTypes{argumentType};
    return FindExactOperation(UnaryOperators_[opcode], argumentTypes, resultType);
}

std::optional<TResolvedOperation> TExpressionRegistry::FindBinary(
    EBinaryOp opcode,
    EValueType lhsType,
    EValueType rhsType,
    EValueType resultType) const
{
    if (!TEnumTraits<EBinaryOp>::IsKnownValue(opcode)) {
        return std::nullopt;
    }

    std::array argumentTypes{lhsType, rhsType};
    return FindExactOperation(BinaryOperators_[opcode], argumentTypes, resultType);
}

////////////////////////////////////////////////////////////////////////////////

void TExpressionRegistryBuilder::RegisterFunction(
    std::string functionName,
    TOperationDescriptor descriptor)
{
    if (functionName.empty()) {
        THROW_ERROR_EXCEPTION("Portable function name cannot be empty");
    }

    auto operation = Format("function %Qv", functionName);
    ValidateDescriptor(descriptor, operation, std::nullopt);

    if (VariadicFunctions_.find(functionName) != VariadicFunctions_.end()) {
        THROW_ERROR_EXCEPTION("Portable %v cannot mix exact and variadic overloads", operation);
    }

    ValidateAndAppendDescriptor(&Functions_[functionName], std::move(descriptor), operation);
}

void TExpressionRegistryBuilder::RegisterVariadicFunction(
    std::string functionName,
    TVariadicOperationDescriptor descriptor)
{
    if (functionName.empty()) {
        THROW_ERROR_EXCEPTION("Portable function name cannot be empty");
    }

    auto operation = Format("function %Qv", functionName);
    ValidateVariadicDescriptor(descriptor, operation);

    if (Functions_.find(functionName) != Functions_.end()) {
        THROW_ERROR_EXCEPTION("Portable %v cannot mix exact and variadic overloads", operation);
    }

    if (VariadicFunctions_.find(functionName) != VariadicFunctions_.end()) {
        THROW_ERROR_EXCEPTION("Portable %v variadic overload is already registered", operation);
    }

    VariadicFunctions_.emplace(std::move(functionName), std::move(descriptor));
}

void TExpressionRegistryBuilder::RegisterUnary(
    EUnaryOp opcode,
    TOperationDescriptor descriptor)
{
    if (!TEnumTraits<EUnaryOp>::IsKnownValue(opcode)) {
        THROW_ERROR_EXCEPTION("Portable unary operator has invalid opcode %v",
            static_cast<int>(opcode));
    }

    auto operation = Format("unary operator %Qlv", opcode);
    ValidateDescriptor(descriptor, operation, 1);
    ValidateAndAppendDescriptor(&UnaryOperators_[opcode], std::move(descriptor), operation);
}

void TExpressionRegistryBuilder::RegisterBinary(
    EBinaryOp opcode,
    TOperationDescriptor descriptor)
{
    if (!TEnumTraits<EBinaryOp>::IsKnownValue(opcode)) {
        THROW_ERROR_EXCEPTION("Portable binary operator has invalid opcode %v",
            static_cast<int>(opcode));
    }

    auto operation = Format("binary operator %Qlv", opcode);
    ValidateDescriptor(descriptor, operation, 2);
    ValidateAndAppendDescriptor(&BinaryOperators_[opcode], std::move(descriptor), operation);
}

TExpressionRegistry TExpressionRegistryBuilder::Build() const
{
    TExpressionRegistry registry;
    registry.Functions_ = Functions_;
    registry.VariadicFunctions_ = VariadicFunctions_;
    registry.UnaryOperators_ = UnaryOperators_;
    registry.BinaryOperators_ = BinaryOperators_;
    return registry;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NPortable
