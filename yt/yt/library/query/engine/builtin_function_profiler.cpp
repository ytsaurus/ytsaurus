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
                type == EValueType::Double);

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


class TSimpleAggregateCodegen
    : public IAggregateCodegen
{
public:
    TSimpleAggregateCodegen(const TString& function)
        : Function(function)
    { }

    TString Function;

    TCodegenAggregate Profile(
        EValueType argumentType,
        EValueType stateType,
        EValueType /*resultType*/,
        const TString& name,
        llvm::FoldingSetNodeID* id) const override
    {
        if (id) {
            id->AddString(ToStringRef(Function + "_aggregate"));
        }

        auto iteration = [
            this_ = MakeStrong(this),
            argumentType,
            stateType,
            name
        ] (TCGBaseContext& builder, Value* buffer, TCGValue aggregateValue, TCGValue newValue)
        {
            return CodegenIf<TCGBaseContext, TCGValue>(
                builder,
                builder->CreateNot(newValue.GetIsNull(builder)),
                [&] (TCGBaseContext& builder) {
                    Value* valueLength = nullptr;
                    if (argumentType == EValueType::String) {
                        valueLength = newValue.GetLength();
                    }
                    Value* newData = newValue.GetTypedData(builder);

                    return CodegenIf<TCGBaseContext, TCGValue>(
                        builder,
                        aggregateValue.GetIsNull(builder),
                        [&] (TCGBaseContext& builder) {
                            if (argumentType == EValueType::String) {
                                Value* permanentData = builder->CreateCall(
                                    builder.Module->GetRoutine("AllocateBytes"),
                                    {
                                        buffer,
                                        builder->CreateZExt(valueLength, builder->getInt64Ty())
                                    });
                                builder->CreateMemCpy(
                                    permanentData,
                                    llvm::Align(1),
                                    newData,
                                    llvm::Align(1),
                                    valueLength);
                                return TCGValue::Create(
                                    builder,
                                    builder->getFalse(),
                                    valueLength,
                                    permanentData,
                                    stateType);
                            } else {
                                return newValue;
                            }
                        },
                        [&] (TCGBaseContext& builder) {
                            Value* aggregateData = aggregateValue.GetTypedData(builder);
                            Value* resultData = nullptr;
                            Value* resultLength = nullptr;

                            if (this_->Function == "sum") {
                                switch (argumentType) {
                                    case EValueType::Int64:
                                    case EValueType::Uint64:
                                        resultData = builder->CreateAdd(
                                            aggregateData,
                                            newData);
                                        break;
                                    case EValueType::Double:
                                        resultData = builder->CreateFAdd(
                                            aggregateData,
                                            newData);
                                        break;
                                    default:
                                        YT_UNIMPLEMENTED();
                                }
                            } else if (this_->Function == "min") {
                                Value* compareResult = nullptr;
                                switch (argumentType) {
                                    case EValueType::Int64:
                                        compareResult = builder->CreateICmpSLT(newData, aggregateData);
                                        break;
                                    case EValueType::Uint64:
                                        compareResult = builder->CreateICmpULT(newData, aggregateData);
                                        break;
                                    case EValueType::Boolean:
                                        compareResult = builder->CreateICmpULT(newData, aggregateData);
                                        break;
                                    case EValueType::Double:
                                        compareResult = builder->CreateFCmpULT(newData, aggregateData);
                                        break;
                                    case EValueType::String: {
                                        compareResult = CodegenLexicographicalCompare(
                                            builder,
                                            newData,
                                            valueLength,
                                            aggregateData,
                                            aggregateValue.GetLength());

                                        newData = CodegenIf<TCGBaseContext, Value*>(
                                            builder,
                                            compareResult,
                                            [&] (TCGBaseContext& builder) {
                                                Value* permanentData = builder->CreateCall(
                                                    builder.Module->GetRoutine("AllocateBytes"),
                                                    {
                                                        buffer,
                                                        builder->CreateZExt(valueLength, builder->getInt64Ty())
                                                    });
                                                builder->CreateMemCpy(
                                                    permanentData,
                                                    llvm::Align(1),
                                                    newData,
                                                    llvm::Align(1),
                                                    valueLength);
                                                return permanentData;
                                            },
                                            [&] (TCGBaseContext& /*builder*/) {
                                                return newData;
                                            });
                                        break;
                                    }
                                    default:
                                        YT_UNIMPLEMENTED();
                                }

                                if (argumentType == EValueType::String) {
                                    resultLength = builder->CreateSelect(
                                        compareResult,
                                        valueLength,
                                        aggregateValue.GetLength());
                                }

                                resultData = builder->CreateSelect(
                                    compareResult,
                                    newData,
                                    aggregateData);
                            } else if (this_->Function == "max") {
                                Value* compareResult = nullptr;
                                switch (argumentType) {
                                    case EValueType::Int64:
                                        compareResult = builder->CreateICmpSGT(newData, aggregateData);
                                        break;
                                    case EValueType::Uint64:
                                        compareResult = builder->CreateICmpUGT(newData, aggregateData);
                                        break;
                                    case EValueType::Boolean:
                                        compareResult = builder->CreateICmpUGT(newData, aggregateData);
                                        break;
                                    case EValueType::Double:
                                        compareResult = builder->CreateFCmpUGT(newData, aggregateData);
                                        break;
                                    case EValueType::String: {
                                        compareResult = CodegenLexicographicalCompare(
                                            builder,
                                            aggregateData,
                                            aggregateValue.GetLength(),
                                            newData,
                                            valueLength);

                                        newData = CodegenIf<TCGBaseContext, Value*>(
                                            builder,
                                            compareResult,
                                            [&] (TCGBaseContext& builder) {
                                                Value* permanentData = builder->CreateCall(
                                                    builder.Module->GetRoutine("AllocateBytes"),
                                                    {
                                                        buffer,
                                                        builder->CreateZExt(valueLength, builder->getInt64Ty())
                                                    });
                                                builder->CreateMemCpy(
                                                    permanentData,
                                                    llvm::Align(1),
                                                    newData,
                                                    llvm::Align(1),
                                                    valueLength);
                                                return permanentData;
                                            },
                                            [&] (TCGBaseContext& /*builder*/) {
                                                return newData;
                                            });
                                        break;
                                    }
                                    default:
                                        YT_UNIMPLEMENTED();
                                }

                                if (argumentType == EValueType::String) {
                                    resultLength = builder->CreateSelect(
                                        compareResult,
                                        valueLength,
                                        aggregateValue.GetLength());
                                }

                                resultData = builder->CreateSelect(
                                    compareResult,
                                    newData,
                                    aggregateData);
                            } else {
                                YT_UNIMPLEMENTED();
                            }

                            return TCGValue::Create(
                                builder,
                                builder->getFalse(),
                                resultLength,
                                resultData,
                                stateType);
                        });
                },
                [&] (TCGBaseContext& /*builder*/) {
                    return aggregateValue;
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
        codegenAggregate.Merge = iteration;

        codegenAggregate.Finalize = [
            this_ = MakeStrong(this),
            name
        ] (TCGBaseContext& /*builder*/, Value* /*buffer*/, TCGValue aggState) {
            return aggState;
        };

        return codegenAggregate;
    }

    bool IsFirst() const override
    {
        return false;
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
        std::unordered_map<TTypeArgument, TUnionType> /*typeArgumentConstraints*/,
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
        std::unordered_map<TTypeArgument, TUnionType> /*typeArgumentConstraints*/,
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
        std::unordered_map<TTypeArgument, TUnionType> /*typeArgumentConstraints*/,
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
