#include "builtin_function_profiler.h"

#include "functions_cg.h"
#include "cg_fragment_compiler.h"

#include <yt/yt/library/query/engine_api/range_inferrer.h>
#include <yt/yt/library/query/engine_api/new_range_inferrer.h>

#include <yt/yt/library/query/base/builtin_function_registry.h>
#include <yt/yt/library/query/base/functions.h>
#include <yt/yt/library/query/base/functions_builder.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/constraints.h>

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/resource/resource.h>

#include <llvm/ADT/FoldingSet.h>

#define UDF_BC(name) TSharedRef::FromString(::NResource::Find(TString("/llvm_bc/") + (name)))

namespace NYT::NQueryClient {
namespace NBuiltins {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TIfFunctionCodegen
    : public IFunctionCodegen
{
public:
    static TCGValue CodegenValue(
        TCGExprContext& builder,
        std::vector<size_t> argIds,
        EValueType type,
        const TString& name)
    {
        auto nameTwine = Twine(name.c_str());

        YT_VERIFY(argIds.size() == 3);
        auto condition = CodegenFragment(builder, argIds[0]);

        // TODO(lukyan): Remove this
        if (condition.GetStaticType() == EValueType::Null) {
            return TCGValue::CreateNull(builder, type);
        }

        YT_VERIFY(condition.GetStaticType() == EValueType::Boolean);

        auto codegenIf = [&] (TCGExprContext& builder) {
            return CodegenIf<TCGExprContext, TCGValue>(
                builder,
                condition.GetTypedData(builder),
                [&] (TCGExprContext& builder) {
                    return CodegenFragment(builder, argIds[1]).Cast(builder, type);
                },
                [&] (TCGExprContext& builder) {
                    return CodegenFragment(builder, argIds[2]).Cast(builder, type);
                },
                nameTwine);
        };

        if (builder.ExpressionFragments.Items[argIds[0]].Nullable) {
            return CodegenIf<TCGExprContext, TCGValue>(
                builder,
                condition.GetIsNull(builder),
                [&] (TCGExprContext& builder) {
                    return TCGValue::CreateNull(builder, type);
                },
                codegenIf,
                nameTwine);
        } else {
            return codegenIf(builder);
        }
    }

    TCodegenExpression Profile(
        TCGVariables* /*variables*/,
        std::vector<size_t> argIds,
        std::unique_ptr<bool[]> /*literalArgs*/,
        std::vector<EValueType> /*argumentTypes*/,
        EValueType type,
        const TString& name,
        NCodegen::EExecutionBackend /*executionBackend*/,
        llvm::FoldingSetNodeID* /*id*/) const override
    {
        return [
            =,
            argIds = std::move(argIds)
        ] (TCGExprContext& builder) {
            return CodegenValue(
                builder,
                argIds,
                type,
                name);
        };
    }

    bool IsNullable(const std::vector<bool>& nullableArgs) const override
    {
        YT_VERIFY(nullableArgs.size() == 3);
        return nullableArgs[0] || nullableArgs[1] || nullableArgs[2];
    }

};

TStringBuf GetUpperBound(TStringBuf source, TChunkedMemoryPool* memoryPool)
{
    ui32 length = source.Size();
    while (length > 0 && source[length - 1] == std::numeric_limits<char>::max()) {
        --length;
    }

    if (length > 0) {
        char* newValue = memoryPool->AllocateUnaligned(length);
        memcpy(newValue, source.data(), length);
        ++newValue[length - 1];
        return {newValue, length};
    } else {
        return TStringBuf{};
    }
}

TKeyTriePtr IsPrefixRangeExtractor(
    const TConstFunctionExpressionPtr& expr,
    const TKeyColumns& keyColumns,
    const TRowBufferPtr& rowBuffer)
{
    auto result = TKeyTrie::Universal();
    auto lhsExpr = expr->Arguments[0];
    auto rhsExpr = expr->Arguments[1];

    auto referenceExpr = rhsExpr->As<TReferenceExpression>();
    auto constantExpr = lhsExpr->As<TLiteralExpression>();

    if (referenceExpr && constantExpr) {
        int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);
        if (keyPartIndex >= 0) {
            auto value = TValue(constantExpr->Value);

            YT_VERIFY(value.Type == EValueType::String);

            result = New<TKeyTrie>(keyPartIndex);
            result->Bounds.emplace_back(value, true);
            auto upper = GetUpperBound(TStringBuf{value.Data.String, value.Length}, rowBuffer->GetPool());
            result->Bounds.emplace_back(
                upper.Empty()
                    ? MakeSentinelValue<TUnversionedValue>(EValueType::Max)
                    : MakeUnversionedStringValue(upper),
                false);
        }
    }

    return result;
}

TConstraintRef IsPrefixConstraintExtractor(
    TConstraintsHolder* constraints,
    const TConstFunctionExpressionPtr& expr,
    const TKeyColumns& keyColumns,
    const TRowBufferPtr& rowBuffer)
{
    auto lhsExpr = expr->Arguments[0];
    auto rhsExpr = expr->Arguments[1];

    auto referenceExpr = rhsExpr->As<TReferenceExpression>();
    auto constantExpr = lhsExpr->As<TLiteralExpression>();

    if (referenceExpr && constantExpr) {
        int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);
        if (keyPartIndex >= 0) {
            auto value = TValue(constantExpr->Value);
            YT_VERIFY(value.Type == EValueType::String);

            auto upper = GetUpperBound(TStringBuf{value.Data.String, value.Length}, rowBuffer->GetPool());

            return constraints->Interval(
                TValueBound{value, false},
                TValueBound{
                    upper.Empty()
                        ? MakeSentinelValue<TUnversionedValue>(EValueType::Max)
                        : MakeUnversionedStringValue(upper),
                    false},
                keyPartIndex);
        }
    }

    return TConstraintRef::Universal();
}

class TIsNullCodegen
    : public IFunctionCodegen
{
public:
    TCodegenExpression Profile(
        TCGVariables* /*variables*/,
        std::vector<size_t> argIds,
        std::unique_ptr<bool[]> /*literalArgs*/,
        std::vector<EValueType> /*argumentTypes*/,
        EValueType type,
        const TString& /*name*/,
        NCodegen::EExecutionBackend /*executionBackend*/,
        llvm::FoldingSetNodeID* /*id*/) const override
    {
        YT_VERIFY(argIds.size() == 1);

        return [
            =,
            argIds = std::move(argIds)
        ] (TCGExprContext& builder) {
            if (builder.ExpressionFragments.Items[argIds[0]].Nullable) {
                auto argValue = CodegenFragment(builder, argIds[0]);
                return TCGValue::Create(
                    builder,
                    builder->getFalse(),
                    nullptr,
                    argValue.GetIsNull(builder),
                    type);
            } else {
                return TCGValue::Create(
                    builder,
                    builder->getFalse(),
                    nullptr,
                    builder->getFalse(),
                    type);
            }
        };
    }

    bool IsNullable(const std::vector<bool>& /*nullableArgs*/) const override
    {
        return false;
    }

};

class TIfNullCodegen
    : public IFunctionCodegen
{
public:
    TCodegenExpression Profile(
        TCGVariables* /*variables*/,
        std::vector<size_t> argIds,
        std::unique_ptr<bool[]> /*literalArgs*/,
        std::vector<EValueType> /*argumentTypes*/,
        EValueType type,
        const TString& /*name*/,
        NCodegen::EExecutionBackend /*executionBackend*/,
        llvm::FoldingSetNodeID* /*id*/) const override
    {
        YT_VERIFY(argIds.size() == 2);

        return [
            =,
            argIds = std::move(argIds)
        ] (TCGExprContext& builder) {
            if (builder.ExpressionFragments.Items[argIds[0]].Nullable) {
                auto argValue = CodegenFragment(builder, argIds[0]);
                auto constant = CodegenFragment(builder, argIds[1]);

                Value* argIsNull = argValue.GetIsNull(builder);

                Value* length = nullptr;

                if (IsStringLikeType(argValue.GetStaticType())) {
                    length = builder->CreateSelect(
                        argIsNull,
                        constant.GetLength(),
                        argValue.GetLength());
                }

                return TCGValue::Create(
                    builder,
                    builder->CreateAnd(argIsNull, constant.GetIsNull(builder)),
                    length,
                    builder->CreateSelect(
                        argIsNull,
                        constant.GetTypedData(builder),
                        argValue.GetTypedData(builder)),
                    type);
            } else {
                return CodegenFragment(builder, argIds[0]);
            }
        };
    }

    bool IsNullable(const std::vector<bool>& nullableArgs) const override
    {
        YT_VERIFY(nullableArgs.size() == 2);
        return nullableArgs[1];
    }

};

class TIsNaNCodegen
    : public IFunctionCodegen
{
public:
    TCodegenExpression Profile(
        TCGVariables* /*variables*/,
        std::vector<size_t> argIds,
        std::unique_ptr<bool[]> /*literalArgs*/,
        std::vector<EValueType> /*argumentTypes*/,
        EValueType type,
        const TString& /*name*/,
        NCodegen::EExecutionBackend /*executionBackend*/,
        llvm::FoldingSetNodeID* /*id*/) const override
    {
        YT_VERIFY(argIds.size() == 1);

        return [
            =,
            argIds = std::move(argIds)
        ] (TCGExprContext& builder) {
            auto argValue = CodegenFragment(builder, argIds[0]);
            Value* data = CodegenFragment(builder, argIds[0]).GetTypedData(builder);
            if (builder.ExpressionFragments.Items[argIds[0]].Nullable) {
                return TCGValue::Create(
                    builder,
                    builder->getFalse(),
                    nullptr,
                    builder->CreateAnd(
                        builder->CreateNot(argValue.GetIsNull(builder)),
                        builder->CreateFCmpUNO(data, data)),
                    type);
            } else {
                return TCGValue::Create(
                    builder,
                    builder->getFalse(),
                    nullptr,
                    builder->CreateFCmpUNO(data, data),
                    type);
            }
        };
    }

    bool IsNullable(const std::vector<bool>& /*nullableArgs*/) const override
    {
        return false;
    }

};

class TCoalesceCodegen
    : public IFunctionCodegen
{
public:
    TCodegenExpression Profile(
        TCGVariables* /*variables*/,
        std::vector<size_t> argIds,
        std::unique_ptr<bool[]> /*literalArgs*/,
        std::vector<EValueType> /*argumentTypes*/,
        EValueType type,
        const TString& /*name*/,
        NCodegen::EExecutionBackend /*executionBackend*/,
        llvm::FoldingSetNodeID* /*id*/) const override
    {
        return [
            =,
            argIds = std::move(argIds)
        ] (TCGExprContext& builder) -> TCGValue {
            return CoalesceRecursive(
                builder,
                argIds.begin(),
                argIds.end(),
                type);
        };
    }

private:
    static TCGValue CoalesceRecursive(
        TCGExprContext& builder,
        std::vector<size_t>::const_iterator begin,
        std::vector<size_t>::const_iterator end,
        EValueType type)
    {
        if (begin == end) {
            return TCGValue::CreateNull(builder, type);
        }

        auto argument = CodegenFragment(builder, *begin);

        if (!builder.ExpressionFragments.Items[*begin].Nullable) {
            return argument;
        }

        Value* argIsNull = argument.GetIsNull(builder);
        return CodegenIf<TCGExprContext, TCGValue>(
            builder,
            argIsNull,
            [&] (TCGExprContext& builder) {
                return CoalesceRecursive(builder, std::next(begin), end, type);
            },
            [&] (TCGExprContext& /*builder*/) {
                return argument;
            }
        );
    }
};

class TUserCastCodegen
    : public IFunctionCodegen
{
public:

    TUserCastCodegen()
    { }

    TCodegenExpression Profile(
        TCGVariables* /*variables*/,
        std::vector<size_t> argIds,
        std::unique_ptr<bool[]> /*literalArgs*/,
        std::vector<EValueType> argumentTypes,
        EValueType type,
        const TString& /*name*/,
        NCodegen::EExecutionBackend /*executionBackend*/,
        llvm::FoldingSetNodeID* /*id*/) const override
    {
        YT_VERIFY(argIds.size() == 1);

        if (argumentTypes[0] == EValueType::Any) {
            return [
                =,
                argIds = std::move(argIds)
            ] (TCGExprContext& builder) {
                auto unversionedValueType = TTypeBuilder<TValue>::Get(builder->getContext());

                auto resultPtr = builder->CreateAlloca(unversionedValueType, nullptr, "resultPtr");
                auto valuePtr = builder->CreateAlloca(unversionedValueType);

                auto cgValue = CodegenFragment(builder, argIds[0]);
                cgValue.StoreToValue(builder, valuePtr);

                const char* routineName = nullptr;

                switch (type) {
                    case EValueType::Int64:
                        routineName = "AnyToInt64";
                        break;
                    case EValueType::Uint64:
                        routineName = "AnyToUint64";
                        break;
                    case EValueType::Double:
                        routineName = "AnyToDouble";
                        break;
                    case EValueType::Boolean:
                        routineName = "AnyToBoolean";
                        break;
                    case EValueType::String:
                        routineName = "AnyToString";
                        break;
                    default:
                        YT_ABORT();
                }

                builder->CreateCall(
                    builder.Module->GetRoutine(routineName),
                    {
                        builder.Buffer,
                        resultPtr,
                        valuePtr
                    });

                return TCGValue::LoadFromRowValue(
                    builder,
                    resultPtr,
                    type);
            };
        } else {
            YT_VERIFY(
                type == EValueType::Int64 ||
                type == EValueType::Uint64 ||
                type == EValueType::Double ||
                type == EValueType::Boolean ||
                type == EValueType::String);

            return [
                =,
                argIds = std::move(argIds)
            ] (TCGExprContext& builder) {
                return CodegenFragment(builder, argIds[0]).Cast(builder, type);
            };
        }
    }

    bool IsNullable(const std::vector<bool>& nullableArgs) const override
    {
        YT_VERIFY(nullableArgs.size() == 1);
        return nullableArgs[0];
    }

};

Value* CodegenCompare(
    TCGBaseContext& builder,
    EValueType type,
    Value* lhs,
    Value* rhs,
    Value* lhsLength = nullptr,
    Value* rhsLength = nullptr)
{
    Value* compareResult = nullptr;
    switch (type) {
        case EValueType::Int64:
            compareResult = builder->CreateICmpSLT(lhs, rhs);
            break;
        case EValueType::Uint64:
        case EValueType::Boolean:
            compareResult = builder->CreateICmpULT(lhs, rhs);
            break;
        case EValueType::Double:
            compareResult = builder->CreateFCmpULT(lhs, rhs);
            break;
        case EValueType::String:
            compareResult = CodegenLexicographicalCompare(
                builder,
                lhs,
                lhsLength,
                rhs,
                rhsLength);
            break;
        default:
            YT_UNIMPLEMENTED();
    }
    return compareResult;
}

Value* CodegenCopyString(TCGBaseContext& builder, Value* buffer, Value* data, Value* length)
{
    Value* permanentData = builder->CreateCall(
        builder.Module->GetRoutine("AllocateBytes"),
        {
            buffer,
            builder->CreateZExt(length, builder->getInt64Ty())
        });
    builder->CreateMemCpy(
        permanentData,
        llvm::Align(1),
        data,
        llvm::Align(1),
        length);
    return permanentData;
}

class TSimpleAggregateCodegen
    : public IAggregateCodegen
{
public:
    explicit TSimpleAggregateCodegen(const TString& function)
        : Function_(function)
    { }

    TCodegenAggregate Profile(
        std::vector<EValueType> argumentTypes,
        EValueType stateType,
        EValueType /*resultType*/,
        const TString& name,
        NCodegen::EExecutionBackend /*executionBackend*/,
        llvm::FoldingSetNodeID* id) const override
    {
        YT_VERIFY(argumentTypes.size() == 1);

        EValueType argumentType = argumentTypes[0];

        if (id) {
            id->AddString(ToStringRef(Function_ + "_aggregate"));
        }

        auto iteration = [
            this_ = MakeStrong(this),
            argumentType,
            name
        ] (TCGBaseContext& builder, Value* buffer, TCGValue aggregateValue, std::vector<TCGValue> newValues) {
            YT_VERIFY(newValues.size() == 1);
            TCGValue newValue = newValues[0];

            return CodegenIf<TCGBaseContext, TCGValue>(
                builder,
                builder->CreateNot(newValue.GetIsNull(builder)),
                [&] (TCGBaseContext& builder) {
                    return CodegenIf<TCGBaseContext, TCGValue>(
                        builder,
                        aggregateValue.GetIsNull(builder),
                        [&] (TCGBaseContext& builder) {
                            return this_->InitializeAggregateValue(
                                builder,
                                buffer,
                                newValue,
                                argumentType);
                        },
                        [&] (TCGBaseContext& builder) {
                            return this_->UpdateAggregateValue(
                                builder,
                                buffer,
                                aggregateValue,
                                newValue,
                                argumentType);
                        });
                },
                [aggregateValue] (TCGBaseContext& /*builder*/) {
                    return aggregateValue;
                });
        };

        auto merge = [
            this_ = MakeStrong(this),
            argumentType,
            name
        ] (TCGBaseContext& builder, Value* buffer, TCGValue aggState, TCGValue dstAggState) {
            return CodegenIf<TCGBaseContext, TCGValue>(
                builder,
                dstAggState.GetIsNull(builder),
                [&] (TCGBaseContext& /*builder*/) {
                    return aggState;
                },
                [&] (TCGBaseContext& builder) {
                    return CodegenIf<TCGBaseContext, TCGValue>(
                        builder,
                        aggState.GetIsNull(builder),
                        [&] (TCGBaseContext& builder) {
                            return this_->InitializeAggregateValue(builder, buffer, dstAggState, argumentType);
                        },
                        [&] (TCGBaseContext& builder) {
                            return this_->UpdateAggregateValue(builder, buffer, aggState, dstAggState, argumentType);
                        });
                });
        };

        TCodegenAggregate codegenAggregate;
        codegenAggregate.Initialize = [
            this_ = MakeStrong(this),
            stateType,
            name
        ] (TCGBaseContext& builder, Value* /*buffer*/) {
            return TCGValue::CreateNull(builder, stateType);
        };

        codegenAggregate.Update = iteration;
        codegenAggregate.Merge = merge;

        codegenAggregate.Finalize = [] (TCGBaseContext& /*builder*/, Value* /*buffer*/, TCGValue aggState)
        {
            return aggState;
        };

        return codegenAggregate;
    }

    bool IsFirst() const override
    {
        return false;
    }

    TCGValue InitializeAggregateValue(
        TCGBaseContext& builder,
        Value* buffer,
        TCGValue newValue,
        EValueType argumentType) const
    {
        if (argumentType != EValueType::String) {
            return newValue;
        }

        Value* stringLength = newValue.GetLength();
        Value* permanentData = CodegenCopyString(
            builder,
            buffer,
            newValue.GetTypedData(builder),
            stringLength);

        return TCGValue::Create(
            builder,
            builder->getFalse(),
            stringLength,
            permanentData,
            EValueType::String);
    };

    TCGValue UpdateAggregateValue(
        TCGBaseContext& builder,
        Value* buffer,
        TCGValue aggregate,
        TCGValue newValue,
        EValueType argumentType) const
    {
        Value* newData = newValue.GetTypedData(builder);
        Value* aggregateData = aggregate.GetTypedData(builder);
        Value* newLength = newValue.GetLength();
        Value* aggregateLength = aggregate.GetLength();

        if (Function_ == "sum") {
            switch (argumentType) {
                case EValueType::Int64:
                case EValueType::Uint64:
                    aggregateData = builder->CreateAdd(aggregateData, newData);
                    break;
                case EValueType::Double:
                    aggregateData = builder->CreateFAdd(aggregateData, newData);
                    break;
                default:
                    YT_UNIMPLEMENTED();
            }
        } else if (Function_ == "min" || Function_ == "max") {
            Value* compareResult;
            if (Function_ == "min") {
                compareResult = CodegenCompare(
                    builder,
                    argumentType,
                    newData,
                    aggregateData,
                    newLength,
                    aggregateLength);
            } else {
                compareResult = CodegenCompare(
                    builder,
                    argumentType,
                    aggregateData,
                    newData,
                    aggregateLength,
                    newLength);
            }

            if (argumentType == EValueType::String) {
                aggregateData = CodegenIf<TCGBaseContext, Value*>(
                    builder,
                    compareResult,
                    [&] (TCGBaseContext& builder){
                        return CodegenCopyString(builder, buffer, newData, newLength);
                    },
                    [&] (TCGBaseContext& /*builder*/){
                        return aggregateData;
                    });
                aggregateLength = builder->CreateSelect(compareResult, newLength, aggregateLength);
            } else {
                aggregateData = builder->CreateSelect(compareResult, newData, aggregateData);
            }
        } else {
            YT_UNIMPLEMENTED();
        }

        return TCGValue::Create(
            builder,
            builder->getFalse(),
            aggregateLength,
            aggregateData,
            argumentType);
    }

private:
    const TString Function_;
};

class TArgMinMaxAggregateCodegen
    : public IAggregateCodegen
{
public:
    explicit TArgMinMaxAggregateCodegen(const TString& function)
        : Function_(function)
    { }

    TCodegenAggregate Profile(
        std::vector<EValueType> argumentTypes,
        EValueType stateType,
        EValueType /*resultType*/,
        const TString& name,
        NCodegen::EExecutionBackend /*executionBackend*/,
        llvm::FoldingSetNodeID* id) const override
    {
        YT_VERIFY(argumentTypes.size() == 2 && stateType == EValueType::String);

        if (id) {
            id->AddString(ToStringRef(Function_ + "_aggregate"));
        }

        auto iteration = [
            this_ = MakeStrong(this),
            argumentTypes,
            name
        ] (TCGBaseContext& builder, Value* buffer, TCGValue aggregate, std::vector<TCGValue> newValues) {
            YT_VERIFY(newValues.size() == 2);

            Value* anyArgIsNull = builder->CreateOr(newValues[0].GetIsNull(builder), newValues[1].GetIsNull(builder));

            return CodegenIf<TCGBaseContext, TCGValue>(
                builder,
                builder->CreateNot(anyArgIsNull),
                [&] (TCGBaseContext& builder) {
                    return CodegenIf<TCGBaseContext, TCGValue>(
                        builder,
                        aggregate.GetIsNull(builder),
                        [&] (TCGBaseContext& builder) {
                            return this_->InitializeAggregateValue(
                                builder,
                                buffer,
                                newValues,
                                argumentTypes);
                        },
                        [&] (TCGBaseContext& builder) {
                            return this_->UpdateAggregateValue(
                                builder,
                                buffer,
                                aggregate,
                                newValues,
                                argumentTypes);
                        });
                },
                [aggregate] (TCGBaseContext& /*builder*/) {
                    return aggregate;
                });
        };

        auto merge = [
            this_ = MakeStrong(this),
            argumentTypes,
            name
        ] (TCGBaseContext& builder, Value* buffer, TCGValue aggState, TCGValue dstAggState) {
            return CodegenIf<TCGBaseContext, TCGValue>(
                builder,
                dstAggState.GetIsNull(builder),
                [&] (TCGBaseContext& /*builder*/) {
                    return aggState;
                },
                [&] (TCGBaseContext& builder) {
                    return CodegenIf<TCGBaseContext, TCGValue>(
                        builder,
                        aggState.GetIsNull(builder),
                        [&] (TCGBaseContext& builder) {
                            return PackValues(
                                builder,
                                buffer,
                                UnpackValues(
                                    builder,
                                    dstAggState,
                                    argumentTypes),
                                argumentTypes);
                        },
                        [&] (TCGBaseContext& builder) {
                            return this_->MergeTwoAggStates(builder, buffer, aggState, dstAggState, argumentTypes);
                        });
                });
        };

        TCodegenAggregate codegenAggregate;
        codegenAggregate.Initialize = [
            this_ = MakeStrong(this),
            stateType,
            name
        ] (TCGBaseContext& builder, Value* /*buffer*/) {
            return TCGValue::CreateNull(builder, stateType);
        };

        codegenAggregate.Update = iteration;
        codegenAggregate.Merge = merge;

        codegenAggregate.Finalize = [
            this_ = MakeStrong(this),
            name,
            argumentTypes
        ] (TCGBaseContext& builder, Value* buffer, TCGValue aggState) {
            return this_->Finalize(builder, buffer, aggState, argumentTypes);
        };

        return codegenAggregate;
    }

    bool IsFirst() const override
    {
        return false;
    }

    TCGValue InitializeAggregateValue(
        TCGBaseContext& builder,
        Value* buffer,
        const std::vector<TCGValue>& newValues,
        const std::vector<EValueType>& argumentTypes) const
    {
        if (Function_ == "argmin" || Function_ == "argmax") {
            return PackValues(builder, buffer, newValues, argumentTypes);
        } else {
            YT_UNIMPLEMENTED();
        }
    };

    TCGValue UpdateAggregateValue(
        TCGBaseContext& builder,
        Value* buffer,
        TCGValue aggregate,
        const std::vector<TCGValue>& newValues,
        const std::vector<EValueType>& argumentTypes) const
    {
        if (Function_ == "argmin" || Function_ == "argmax") {
            Value* newData = newValues[1].GetTypedData(builder);
            Value* newLength = newValues[1].GetLength();

            auto unpacked = UnpackValues(builder, aggregate, argumentTypes);

            Value* aggregateData = unpacked[1].GetTypedData(builder);
            Value* aggregateLength = unpacked[1].GetLength();

            Value* compareResult;
            if (Function_ == "argmin"){
                compareResult = CodegenCompare(
                    builder,
                    argumentTypes[1],
                    newData,
                    aggregateData,
                    newLength,
                    aggregateLength);
            } else {
                compareResult = CodegenCompare(
                    builder,
                    argumentTypes[1],
                    aggregateData,
                    newData,
                    aggregateLength,
                    newLength);
            }

            return CodegenIf<TCGBaseContext, TCGValue>(
                builder,
                compareResult,
                [&](TCGBaseContext& builder){
                    if (AllScalar(argumentTypes)) {
                        return PackValues(builder, buffer, newValues, argumentTypes, &aggregate);
                    } else {
                        return PackValues(builder, buffer, newValues, argumentTypes);
                    }
                },
                [&](TCGBaseContext& /*builder*/){
                    return aggregate;
                }
            );
        } else {
            YT_UNIMPLEMENTED();
        }
    }

    TCGValue MergeTwoAggStates(
        TCGBaseContext& builder,
        Value* buffer,
        TCGValue aggState,
        TCGValue dstAggState,
        const std::vector<EValueType>& argumentTypes) const
    {
        if (Function_ == "argmin" || Function_ == "argmax") {
            std::vector<TCGValue> dataUnpacked = UnpackValues(builder, aggState, argumentTypes);
            std::vector<TCGValue> dstDataUnpacked = UnpackValues(builder, dstAggState, argumentTypes);

            Value* compareResult = CodegenCompare(
                builder,
                argumentTypes[1],
                dataUnpacked[1].GetTypedData(builder),
                dstDataUnpacked[1].GetTypedData(builder),
                dataUnpacked[1].GetLength(),
                dstDataUnpacked[1].GetLength());

            if (Function_ == "argmax") {
                compareResult = builder->CreateNot(compareResult);
            }

            return CodegenIf<TCGBaseContext, TCGValue>(
                builder,
                compareResult,
                [&] (TCGBaseContext& /*builder*/) {
                    return aggState;
                },
                [&] (TCGBaseContext& /*builder*/) {
                    return PackValues(builder, buffer, dstDataUnpacked, argumentTypes);
                }
            );
        } else {
            YT_UNIMPLEMENTED();
        }
    }

    TCGValue Finalize(
        TCGBaseContext& builder,
        Value* /*buffer*/,
        TCGValue aggState,
        const std::vector<EValueType>& argumentTypes) const
    {
        if (Function_ == "argmin" || Function_ == "argmax") {
            return UnpackValues(builder, aggState, argumentTypes).front();
        } else {
            YT_UNIMPLEMENTED();
        }
    }

private:
    const TString Function_;

    static bool AllScalar(const std::vector<EValueType>& argTypes)
    {
        for (auto type : argTypes) {
            if (IsStringLikeType(type)) {
                return false;
            }
        }
        return true;
    }

    static TCGValue PackValues(
        TCGBaseContext& builder,
        Value* buffer,
        const std::vector<TCGValue>& args,
        const std::vector<EValueType>& argTypes,
        TCGValue* reuseState = nullptr)
    {
        YT_VERIFY(args.size() == argTypes.size());

        Value* stateSize;
        Value* permanentData;

        if (reuseState == nullptr) {
            stateSize = builder->getInt32(0);

            for (int index = 0; index < std::ssize(args); ++index) {
                switch (argTypes[index]) {
                    case EValueType::String:
                        stateSize = builder->CreateAdd(stateSize, args[index].GetLength());
                        stateSize = builder->CreateAdd(stateSize, builder->getInt32(4));
                        break;
                    case EValueType::Int64:
                    case EValueType::Uint64:
                    case EValueType::Double:
                    case EValueType::Boolean:
                        stateSize = builder->CreateAdd(stateSize, builder->getInt32(8));
                        break;
                    default:
                        YT_UNIMPLEMENTED();
                }
            }

            permanentData = builder->CreateCall(
                builder.Module->GetRoutine("AllocateBytes"),
                {buffer, builder->CreateZExt(stateSize, builder->getInt64Ty())});
        } else {
            stateSize = reuseState->GetLength();
            permanentData = reuseState->GetTypedData(builder);
        }

        Value* iterator = permanentData;

        for (int index = 0; index < std::ssize(args); ++index) {
            switch (argTypes[index]) {
                case EValueType::String: {
                    builder->CreateStore(args[index].GetLength(), iterator);
                    iterator = builder->CreateGEP(builder->getInt8Ty(), iterator, builder->getInt64(4));
                    builder->CreateMemCpy(
                        iterator,
                        llvm::Align(1),
                        args[index].GetTypedData(builder),
                        llvm::Align(1),
                        args[index].GetLength());
                    iterator = builder->CreateGEP(builder->getInt8Ty(), iterator, args[index].GetLength());
                    break;
                }
                case EValueType::Double:
                    builder->CreateStore(
                        builder->CreateZExt(args[index].GetTypedData(builder), builder->getDoubleTy()),
                        iterator);
                    iterator = builder->CreateGEP(builder->getInt8Ty(), iterator, builder->getInt32(8));
                    break;
                case EValueType::Int64:
                case EValueType::Uint64:
                case EValueType::Boolean:
                    builder->CreateStore(
                        builder->CreateZExt(args[index].GetTypedData(builder), builder->getInt64Ty()),
                        iterator);
                    iterator = builder->CreateGEP(builder->getInt8Ty(), iterator, builder->getInt32(8));
                    break;
                default:
                    YT_UNIMPLEMENTED();
            }
        }

        return TCGValue::Create(
            builder,
            builder->getFalse(),
            stateSize,
            permanentData,
            EValueType::String);
    }

    static std::vector<TCGValue> UnpackValues(
        TCGBaseContext& builder,
        TCGValue span,
        const std::vector<EValueType>& argTypes)
    {
        std::vector<TCGValue> inplaceData;

        Value* iterator = span.GetTypedData(builder);

        for (int index = 0; index < std::ssize(argTypes); ++index) {
            switch (argTypes[index]) {
                case EValueType::String: {
                    Value* length = builder->CreateLoad(builder->getInt32Ty(), iterator);
                    iterator = builder->CreateGEP(builder->getInt8Ty(), iterator, builder->getInt32(4));
                    inplaceData.push_back(
                        TCGValue::Create(
                            builder,
                            builder->getFalse(),
                            length,
                            iterator,
                            EValueType::String));
                    iterator = builder->CreateGEP(builder->getInt8Ty(), iterator, length);
                    break;
                }
                case EValueType::Double: {
                    Value *primitive = builder->CreateLoad(builder->getDoubleTy(), iterator);
                    iterator = builder->CreateGEP(builder->getInt8Ty(), iterator, builder->getInt32(8));
                    inplaceData.push_back(
                        TCGValue::Create(
                            builder,
                            builder->getFalse(),
                            nullptr,
                            primitive,
                            argTypes[index]));
                    break;
                }
                case EValueType::Int64:
                case EValueType::Uint64:
                case EValueType::Boolean: {
                    Value* primitive = builder->CreateLoad(builder->getInt64Ty(), iterator);
                    iterator = builder->CreateGEP(builder->getInt8Ty(), iterator, builder->getInt32(8));
                    inplaceData.push_back(
                        TCGValue::Create(
                            builder,
                            builder->getFalse(),
                            nullptr,
                            primitive,
                            argTypes[index]));
                    break;
                }
                default:
                    YT_UNIMPLEMENTED();
            }
        }

        return inplaceData;
    }
};

} // namespace NBuiltins

////////////////////////////////////////////////////////////////////////////////

class TProfilerFunctionRegistryBuilder
    : public IFunctionRegistryBuilder
{
public:
    TProfilerFunctionRegistryBuilder(
        const TFunctionProfilerMapPtr& functionProfilers,
        const TAggregateProfilerMapPtr& aggregateProfilers)
        : FunctionProfilers_(functionProfilers)
        , AggregateProfilers_(aggregateProfilers)
    { }

    void RegisterFunction(
        const TString& functionName,
        const TString& symbolName,
        std::unordered_map<TTypeParameter, TUnionType> /*typeParameterConstraints*/,
        std::vector<TType> /*argumentTypes*/,
        TType /*repeatedArgType*/,
        TType /*resultType*/,
        TStringBuf implementationFile,
        ECallingConvention callingConvention,
        bool useFunctionContext) override
    {
        if (FunctionProfilers_) {
            FunctionProfilers_->emplace(functionName, New<TExternalFunctionCodegen>(
                functionName,
                symbolName,
                UDF_BC(implementationFile),
                GetCallingConvention(callingConvention),
                TSharedRef(),
                useFunctionContext));
        }
    }

    void RegisterFunction(
        const TString& functionName,
        std::vector<TType> /*argumentTypes*/,
        TType /*resultType*/,
        TStringBuf implementationFile,
        ECallingConvention callingConvention) override
    {
        if (FunctionProfilers_) {
            FunctionProfilers_->emplace(functionName, New<TExternalFunctionCodegen>(
                functionName,
                functionName,
                UDF_BC(implementationFile),
                GetCallingConvention(callingConvention),
                TSharedRef(),
                false));
        }
    }

    void RegisterFunction(
        const TString& functionName,
        std::unordered_map<TTypeParameter, TUnionType> /*typeParameterConstraints*/,
        std::vector<TType> argumentTypes,
        TType repeatedArgType,
        TType /*resultType*/,
        TStringBuf implementationFile) override
    {
        if (FunctionProfilers_) {
            FunctionProfilers_->emplace(functionName, New<TExternalFunctionCodegen>(
                functionName,
                functionName,
                UDF_BC(implementationFile),
                GetCallingConvention(ECallingConvention::UnversionedValue, argumentTypes.size(), repeatedArgType),
                TSharedRef(),
                false));
        }
    }

    void RegisterAggregate(
        const TString& aggregateName,
        std::unordered_map<TTypeParameter, TUnionType> /*typeParameterConstraints*/,
        TType /*argumentType*/,
        TType /*resultType*/,
        TType /*stateType*/,
        TStringBuf implementationFile,
        ECallingConvention callingConvention,
        bool isFirst) override
    {
        if (AggregateProfilers_) {
            AggregateProfilers_->emplace(aggregateName, New<TExternalAggregateCodegen>(
                aggregateName, UDF_BC(implementationFile), callingConvention, isFirst, TSharedRef()));
        }
    }

private:
    const TFunctionProfilerMapPtr FunctionProfilers_;
    const TAggregateProfilerMapPtr AggregateProfilers_;
};

std::unique_ptr<IFunctionRegistryBuilder> CreateProfilerFunctionRegistryBuilder(
    const TFunctionProfilerMapPtr& functionProfilers,
    const TAggregateProfilerMapPtr& aggregateProfilers)
{
    return std::make_unique<TProfilerFunctionRegistryBuilder>(functionProfilers, aggregateProfilers);
}

////////////////////////////////////////////////////////////////////////////////

TConstRangeExtractorMapPtr CreateBuiltinRangeExtractors()
{
    auto result = New<TRangeExtractorMap>();
    result->emplace("is_prefix", NBuiltins::IsPrefixRangeExtractor);
    return result;
}

const TConstRangeExtractorMapPtr GetBuiltinRangeExtractors()
{
    static const auto builtinRangeExtractors = CreateBuiltinRangeExtractors();
    return builtinRangeExtractors;
}

TConstConstraintExtractorMapPtr CreateBuiltinConstraintExtractors()
{
    auto result = New<TConstraintExtractorMap>();
    result->emplace("is_prefix", NBuiltins::IsPrefixConstraintExtractor);
    return result;
}

const TConstConstraintExtractorMapPtr GetBuiltinConstraintExtractors()
{
    static const auto builtinRangeExtractors = CreateBuiltinConstraintExtractors();
    return builtinRangeExtractors;
}

TConstFunctionProfilerMapPtr CreateBuiltinFunctionProfilers()
{
    auto result = New<TFunctionProfilerMap>();

    result->emplace("if", New<NBuiltins::TIfFunctionCodegen>());

    result->emplace("is_prefix", New<TExternalFunctionCodegen>(
        "is_prefix",
        "is_prefix",
        UDF_BC("is_prefix"),
        GetCallingConvention(ECallingConvention::Simple),
        TSharedRef()));

    result->emplace("is_null", New<NBuiltins::TIsNullCodegen>());
    result->emplace("is_nan", New<NBuiltins::TIsNaNCodegen>());
    result->emplace("int64", New<NBuiltins::TUserCastCodegen>());
    result->emplace("uint64", New<NBuiltins::TUserCastCodegen>());
    result->emplace("double", New<NBuiltins::TUserCastCodegen>());
    result->emplace("boolean", New<NBuiltins::TUserCastCodegen>());
    result->emplace("string", New<NBuiltins::TUserCastCodegen>());
    result->emplace("if_null", New<NBuiltins::TIfNullCodegen>());
    result->emplace("coalesce", New<NBuiltins::TCoalesceCodegen>());

    TProfilerFunctionRegistryBuilder builder{result.Get(), nullptr};
    RegisterBuiltinFunctions(&builder);

    return result;
}

const TConstFunctionProfilerMapPtr GetBuiltinFunctionProfilers()
{
    static const auto builtinFunctionProfilers = CreateBuiltinFunctionProfilers();
    return builtinFunctionProfilers;
}

TConstAggregateProfilerMapPtr CreateBuiltinAggregateProfilers()
{
    auto result = New<TAggregateProfilerMap>();

    result->emplace("sum", New<NBuiltins::TSimpleAggregateCodegen>("sum"));

    for (const auto& name : {"min", "max"}) {
        result->emplace(name, New<NBuiltins::TSimpleAggregateCodegen>(name));
    }

    for (const auto& name : {"argmin", "argmax"}) {
        result->emplace(name, New<NBuiltins::TArgMinMaxAggregateCodegen>(name));
    }

    TProfilerFunctionRegistryBuilder builder{nullptr, result.Get()};
    RegisterBuiltinFunctions(&builder);

    return result;
}

const TConstAggregateProfilerMapPtr GetBuiltinAggregateProfilers()
{
    static const auto builtinAggregateProfilers = CreateBuiltinAggregateProfilers();
    return builtinAggregateProfilers;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
