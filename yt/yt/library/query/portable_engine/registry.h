#pragma once

#include "public.h"

#include <yt/yt/library/query/base/functions_common.h>
#include <yt/yt/library/query/base/query_common.h>

#include <yt/yt/core/actions/callback.h>

#include <library/cpp/yt/containers/enum_indexed_array.h>

#include <library/cpp/yt/memory/range.h>

#include <util/generic/hash.h>

#include <optional>
#include <string>
#include <vector>

namespace NYT::NQueryClient::NPortable {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EOperationNullPolicy,
    (Propagate)
    (PassToCallback)
);

//! Callbacks may run concurrently. String-like results must be captured by |rowBuffer|.
using TOperationCallback = TCallback<void(
    TValue* result,
    TRange<TValue> arguments,
    const TRowBufferPtr& rowBuffer)>;

struct TOperationImplementation
{
    EOperationNullPolicy NullPolicy = EOperationNullPolicy::Propagate;
    TOperationCallback Callback;
};

struct TResolvedOperation
{
    TOperationImplementation Implementation;
    ssize_t ArgumentCount = 0;
};

struct TOperationDescriptor
{
    std::vector<EValueType> ArgumentTypes;
    EValueType ResultType = EValueType::TheBottom;
    TOperationImplementation Implementation;
};

struct TVariadicOperationDescriptor
{
    TTypeSet AllowedArgumentTypes;
    EValueType ResultType = EValueType::TheBottom;
    TOperationImplementation Implementation;
};

void InvokeOperation(
    const TResolvedOperation& operation,
    TValue* result,
    TRange<TValue> arguments,
    const TRowBufferPtr& rowBuffer);

////////////////////////////////////////////////////////////////////////////////

class TExpressionRegistry
{
public:
    TExpressionRegistry() = default;

    std::optional<TResolvedOperation> FindFunction(
        const std::string& functionName,
        TRange<EValueType> argumentTypes,
        EValueType resultType) const;

    std::optional<TResolvedOperation> FindUnary(
        EUnaryOp opcode,
        EValueType argumentType,
        EValueType resultType) const;

    std::optional<TResolvedOperation> FindBinary(
        EBinaryOp opcode,
        EValueType lhsType,
        EValueType rhsType,
        EValueType resultType) const;

private:
    using TOperationOverloads = std::vector<TOperationDescriptor>;
    using TFunctionOverloads = THashMap<std::string, TOperationOverloads>;
    using TVariadicFunctions = THashMap<std::string, TVariadicOperationDescriptor>;
    using TUnaryOverloads = TEnumIndexedArray<EUnaryOp, TOperationOverloads>;
    using TBinaryOverloads = TEnumIndexedArray<EBinaryOp, TOperationOverloads>;

    TFunctionOverloads Functions_;
    TVariadicFunctions VariadicFunctions_;
    TUnaryOverloads UnaryOperators_;
    TBinaryOverloads BinaryOperators_;

    friend class TExpressionRegistryBuilder;
};

////////////////////////////////////////////////////////////////////////////////

class TExpressionRegistryBuilder
{
public:
    void RegisterFunction(
        std::string functionName,
        TOperationDescriptor descriptor);

    void RegisterVariadicFunction(
        std::string functionName,
        TVariadicOperationDescriptor descriptor);

    void RegisterUnary(
        EUnaryOp opcode,
        TOperationDescriptor descriptor);

    void RegisterBinary(
        EBinaryOp opcode,
        TOperationDescriptor descriptor);

    TExpressionRegistry Build() const;

private:
    TExpressionRegistry::TFunctionOverloads Functions_;
    TExpressionRegistry::TVariadicFunctions VariadicFunctions_;
    TExpressionRegistry::TUnaryOverloads UnaryOperators_;
    TExpressionRegistry::TBinaryOverloads BinaryOperators_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NPortable
