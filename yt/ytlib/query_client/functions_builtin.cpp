#include "functions.h"
#include "functions_cg.h"
#include "cg_fragment_compiler.h"
#include "query.h"
#include "helpers.h"
#include "functions_builder.h"

#ifdef YT_IN_ARCADIA
#include <library/resource/resource.h>
#else
#include "udf/is_prefix.h" // Y_IGNORE
#include "udf/avg.h" // Y_IGNORE
#include "udf/farm_hash.h" // Y_IGNORE
#include "udf/first.h" // Y_IGNORE
#include "udf/hyperloglog.h" // Y_IGNORE
#include "udf/is_substr.h" // Y_IGNORE
#include "udf/lower.h" // Y_IGNORE
#include "udf/concat.h" // Y_IGNORE
#include "udf/max.h" // Y_IGNORE
#include "udf/min.h" // Y_IGNORE
#include "udf/regex.h" // Y_IGNORE
#include "udf/sleep.h" // Y_IGNORE
#include "udf/sum.h" // Y_IGNORE
#include "udf/dates.h" // Y_IGNORE
#include "udf/ypath_get.h" // Y_IGNORE
#endif

namespace NYT {
namespace NQueryClient {
namespace NBuiltins {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TIfFunctionCodegen
    : public IFunctionCodegen
{
public:
    static TCGValue CodegenValue(
        TCGExprContext& builder,
        const std::vector<TCodegenExpression>& codegenArgs,
        EValueType type,
        const TString& name,
        Value* row)
    {
        auto nameTwine = Twine(name.c_str());

        YCHECK(codegenArgs.size() == 3);
        auto condition = codegenArgs[0](builder, row);

        if (condition.GetStaticType() == EValueType::Null) {
            return TCGValue::CreateNull(builder, type);
        }

        YCHECK(condition.GetStaticType() == EValueType::Boolean);

        return CodegenIf<TCGExprContext, TCGValue>(
            builder,
            condition.IsNull(),
            [&] (TCGExprContext& builder) {
                return TCGValue::CreateNull(builder, type);
            },
            [&] (TCGExprContext& builder) {
                return CodegenIf<TCGExprContext, TCGValue>(
                    builder,
                    builder->CreateICmpNE(
                        builder->CreateZExtOrBitCast(condition.GetData(), builder->getInt64Ty()),
                        builder->getInt64(0)),
                    [&] (TCGExprContext& builder) {
                        return codegenArgs[1](builder, row).Cast(builder, type);
                    },
                    [&] (TCGExprContext& builder) {
                        return codegenArgs[2](builder, row).Cast(builder, type);
                    });
            },
            nameTwine);
    }

    virtual TCodegenExpression Profile(
        TCodegenValue codegenFunctionContext,
        std::vector<TCodegenExpression> codegenArgs,
        std::vector<EValueType> argumentTypes,
        EValueType type,
        const TString& name,
        llvm::FoldingSetNodeID* id) const override
    {
        return [
            MOVE(codegenArgs),
            type,
            name
        ] (TCGExprContext& builder, Value* row) {
            return CodegenValue(
                builder,
                codegenArgs,
                type,
                name,
                row);
        };
    }
};

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

            YCHECK(value.Type == EValueType::String);

            result = New<TKeyTrie>(keyPartIndex);
            result->Bounds.emplace_back(value, true);

            ui32 length = value.Length;
            while (length > 0 && value.Data.String[length - 1] == std::numeric_limits<char>::max()) {
                --length;
            }

            if (length > 0) {
                char* newValue = rowBuffer->GetPool()->AllocateUnaligned(length);
                memcpy(newValue, value.Data.String, length);
                ++newValue[length - 1];

                value.Length = length;
                value.Data.String = newValue;
            } else {
                value = MakeSentinelValue<TUnversionedValue>(EValueType::Max);
            }
            result->Bounds.emplace_back(value, false);
        }
    }

    return result;
}

class TIsNullCodegen
    : public IFunctionCodegen
{
public:
    virtual TCodegenExpression Profile(
        TCodegenValue codegenFunctionContext,
        std::vector<TCodegenExpression> codegenArgs,
        std::vector<EValueType> argumentTypes,
        EValueType type,
        const TString& name,
        llvm::FoldingSetNodeID* id) const override
    {
        YCHECK(codegenArgs.size() == 1);

        return [
            MOVE(codegenArgs),
            type,
            name
        ] (TCGExprContext& builder, Value* row) {
            auto argValue = codegenArgs[0](builder, row);
            return TCGValue::CreateFromValue(
                builder,
                builder->getInt1(false),
                nullptr,
                builder->CreateZExtOrBitCast(
                    argValue.IsNull(),
                    TDataTypeBuilder::TBoolean::get(builder->getContext())),
                type);
        };
    }
};

class TIfNullCodegen
    : public IFunctionCodegen
{
public:
    virtual TCodegenExpression Profile(
        TCodegenValue codegenFunctionContext,
        std::vector<TCodegenExpression> codegenArgs,
        std::vector<EValueType> argumentTypes,
        EValueType type,
        const TString& name,
        llvm::FoldingSetNodeID* id) const override
    {
        YCHECK(codegenArgs.size() == 2);

        return [
            MOVE(codegenArgs),
            type,
            name
        ] (TCGExprContext& builder, Value* row) {
            auto argValue = codegenArgs[0](builder, row);
            auto constant = codegenArgs[1](builder, row);

            return TCGValue::CreateFromValue(
                builder,
                builder->CreateOr(argValue.IsNull(), constant.IsNull()),
                nullptr,
                builder->CreateSelect(
                    argValue.IsNull(),
                    constant.GetData(),
                    argValue.GetData()),
                type);
        };
    }
};

class TUserCastCodegen
    : public IFunctionCodegen
{
public:

    TUserCastCodegen()
    { }

    virtual TCodegenExpression Profile(
        TCodegenValue codegenFunctionContext,
        std::vector<TCodegenExpression> codegenArgs,
        std::vector<EValueType> argumentTypes,
        EValueType type,
        const TString& name,
        llvm::FoldingSetNodeID* id) const override
    {
        YCHECK(codegenArgs.size() == 1);

        return [
            MOVE(codegenArgs),
            type,
            name
        ] (TCGExprContext& builder, Value* row) {
            return codegenArgs[0](builder, row).Cast(builder, type);
        };
    }
};

} // namespace NBuiltins

bool IsUserCastFunction(const TString& name)
{
    return name == "int64" || name == "uint64" || name == "double";
}

namespace {

void RegisterBuiltinFunctions(
    const TTypeInferrerMapPtr& typeInferrers,
    const TFunctionProfilerMapPtr& functionProfilers,
    const TAggregateProfilerMapPtr& aggregateProfilers)
{
    TFunctionRegistryBuilder builder(typeInferrers, functionProfilers, aggregateProfilers);

    builder.RegisterFunction(
        "is_substr",
        std::vector<TType>{EValueType::String, EValueType::String},
        EValueType::Boolean,
        UDF_BC(is_substr),
        ECallingConvention::Simple);

    builder.RegisterFunction(
        "lower",
        std::vector<TType>{EValueType::String},
        EValueType::String,
        UDF_BC(lower),
        ECallingConvention::Simple);

    builder.RegisterFunction(
        "concat",
        std::vector<TType>{EValueType::String, EValueType::String},
        EValueType::String,
        UDF_BC(concat),
        ECallingConvention::Simple);

    builder.RegisterFunction(
        "sleep",
        std::vector<TType>{EValueType::Int64},
        EValueType::Int64,
        UDF_BC(sleep),
        ECallingConvention::Simple);

    TUnionType hashTypes = TUnionType{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Boolean,
        EValueType::String};

    builder.RegisterFunction(
        "farm_hash",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{},
        hashTypes,
        EValueType::Uint64,
        UDF_BC(farm_hash));

    if (typeInferrers) {
        typeInferrers->emplace("is_null", New<TFunctionTypeInferrer>(
            std::unordered_map<TTypeArgument, TUnionType>(),
            std::vector<TType>{0},
            EValueType::Null,
            EValueType::Boolean));
    }

    if (functionProfilers) {
        functionProfilers->emplace("is_null", New<NBuiltins::TIsNullCodegen>());
    }

    auto typeArg = 0;
    auto castConstraints = std::unordered_map<TTypeArgument, TUnionType>();
    castConstraints[typeArg] = std::vector<EValueType>{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double};


    if (typeInferrers) {
        typeInferrers->emplace("int64", New<TFunctionTypeInferrer>(
            castConstraints,
            std::vector<TType>{typeArg},
            EValueType::Null,
            EValueType::Int64));
    }

    if (functionProfilers) {
        functionProfilers->emplace("int64", New<NBuiltins::TUserCastCodegen>());
    }

    if (typeInferrers) {
        typeInferrers->emplace("uint64", New<TFunctionTypeInferrer>(
            castConstraints,
            std::vector<TType>{typeArg},
            EValueType::Null,
            EValueType::Uint64));
    }

    if (functionProfilers) {
        functionProfilers->emplace("uint64", New<NBuiltins::TUserCastCodegen>());
    }

    if (typeInferrers) {
        typeInferrers->emplace("double", New<TFunctionTypeInferrer>(
            castConstraints,
            std::vector<TType>{typeArg},
            EValueType::Null,
            EValueType::Double));
    }

    if (functionProfilers) {
        functionProfilers->emplace("double", New<NBuiltins::TUserCastCodegen>());
    }

    if (typeInferrers) {
        typeInferrers->emplace("if_null", New<TFunctionTypeInferrer>(
            std::unordered_map<TTypeArgument, TUnionType>(),
            std::vector<TType>{0, 0},
            0));
    }

    if (functionProfilers) {
        functionProfilers->emplace("if_null", New<NBuiltins::TIfNullCodegen>());
    }


    builder.RegisterFunction(
        "regex_full_match",
        "regex_full_match",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::Boolean,
        UDF_BC(regex),
        New<TUnversionedValueCallingConvention>(-1, true));

    builder.RegisterFunction(
        "regex_partial_match",
        "regex_partial_match",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::Boolean,
        UDF_BC(regex),
        New<TUnversionedValueCallingConvention>(-1, true));

    builder.RegisterFunction(
        "regex_replace_first",
        "regex_replace_first",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::String,
        UDF_BC(regex),
        New<TUnversionedValueCallingConvention>(-1, true));

    builder.RegisterFunction(
        "regex_replace_all",
        "regex_replace_all",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::String,
        UDF_BC(regex),
        New<TUnversionedValueCallingConvention>(-1, true));

    builder.RegisterFunction(
        "regex_extract",
        "regex_extract",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::String,
        UDF_BC(regex),
        New<TUnversionedValueCallingConvention>(-1, true));

    builder.RegisterFunction(
        "regex_escape",
        "regex_escape",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{EValueType::String},
        EValueType::Null,
        EValueType::String,
        UDF_BC(regex),
        New<TUnversionedValueCallingConvention>(-1, true));

    auto constraints = std::unordered_map<TTypeArgument, TUnionType>();
    constraints[typeArg] = std::vector<EValueType>{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double,
        EValueType::String};
    auto sumConstraints = std::unordered_map<TTypeArgument, TUnionType>();
    sumConstraints[typeArg] = std::vector<EValueType>{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double};
    auto anyConstraints = std::unordered_map<TTypeArgument, TUnionType>();
    anyConstraints[typeArg] = std::vector<EValueType>{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Boolean,
        EValueType::Double,
        EValueType::String,
        EValueType::Any};

    builder.RegisterAggregate(
        "first",
        anyConstraints,
        typeArg,
        typeArg,
        typeArg,
        UDF_BC(first),
        ECallingConvention::UnversionedValue);
    builder.RegisterAggregate(
        "sum",
        sumConstraints,
        typeArg,
        typeArg,
        typeArg,
        UDF_BC(sum),
        ECallingConvention::UnversionedValue);
    builder.RegisterAggregate(
        "min",
        constraints,
        typeArg,
        typeArg,
        typeArg,
        UDF_BC(min),
        ECallingConvention::UnversionedValue);
    builder.RegisterAggregate(
        "max",
        constraints,
        typeArg,
        typeArg,
        typeArg,
        UDF_BC(max),
        ECallingConvention::UnversionedValue);
    builder.RegisterAggregate(
        "avg",
        std::unordered_map<TTypeArgument, TUnionType>(),
        EValueType::Int64,
        EValueType::Double,
        EValueType::String,
        UDF_BC(avg),
        ECallingConvention::UnversionedValue);
    builder.RegisterAggregate(
        "cardinality",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<EValueType>{
            EValueType::String,
            EValueType::Uint64,
            EValueType::Int64,
            EValueType::Double,
            EValueType::Boolean},
        EValueType::Uint64,
        EValueType::String,
        UDF_BC(hyperloglog),
        ECallingConvention::UnversionedValue);

    builder.RegisterFunction(
        "format_timestamp",
        std::vector<TType>{EValueType::Int64, EValueType::String},
        EValueType::String,
        UDF_BC(dates),
        ECallingConvention::Simple);

    std::vector<TString> timestampFloorFunctions = {
        "timestamp_floor_hour",
        "timestamp_floor_day",
        "timestamp_floor_week",
        "timestamp_floor_month",
        "timestamp_floor_year"};

    for (const auto& name : timestampFloorFunctions) {
        builder.RegisterFunction(
            name,
            std::vector<TType>{EValueType::Int64},
            EValueType::Int64,
            UDF_BC(dates),
            ECallingConvention::Simple);
    }

    std::vector<std::pair<TString, EValueType>> ypathGetFunctions = {
        {"try_get_int64", EValueType::Int64},
        {"get_int64", EValueType::Int64},
        {"try_get_uint64", EValueType::Uint64},
        {"get_uint64", EValueType::Uint64},
        {"try_get_double", EValueType::Double},
        {"get_double", EValueType::Double},
        {"try_get_boolean", EValueType::Boolean},
        {"get_boolean", EValueType::Boolean},
        {"try_get_string", EValueType::String},
        {"get_string", EValueType::String}};

    for (const auto& fns : ypathGetFunctions) {
        auto&& name = fns.first;
        auto&& type = fns.second;
        builder.RegisterFunction(
            name,
            std::vector<TType>{EValueType::Any, EValueType::String},
            type,
            UDF_BC(ypath_get),
            ECallingConvention::UnversionedValue);
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TConstTypeInferrerMapPtr CreateBuiltinTypeInferrers()
{
    auto result = New<TTypeInferrerMap>();

    result->emplace("if", New<TFunctionTypeInferrer>(
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{ EValueType::Boolean, 0, 0 },
        0));

    result->emplace("is_prefix", New<TFunctionTypeInferrer>(
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{ EValueType::String, EValueType::String },
        EValueType::Boolean));

    RegisterBuiltinFunctions(result.Get(), nullptr, nullptr);

    return result;
}

const TConstTypeInferrerMapPtr BuiltinTypeInferrersMap = CreateBuiltinTypeInferrers();

TConstRangeExtractorMapPtr CreateBuiltinRangeExtractorMap()
{
    auto result = New<TRangeExtractorMap>();
    result->emplace("is_prefix", NBuiltins::IsPrefixRangeExtractor);

    return result;
}

const TConstRangeExtractorMapPtr BuiltinRangeExtractorMap = CreateBuiltinRangeExtractorMap();

TConstFunctionProfilerMapPtr CreateBuiltinFunctionCG()
{
    auto result = New<TFunctionProfilerMap>();

    result->emplace("if", New<NBuiltins::TIfFunctionCodegen>());

    result->emplace("is_prefix", New<TExternalFunctionCodegen>(
        "is_prefix",
        "is_prefix",
        UDF_BC(is_prefix),
        GetCallingConvention(ECallingConvention::Simple),
        TSharedRef()));

    RegisterBuiltinFunctions(nullptr, result.Get(), nullptr);

    return result;
}

const TConstFunctionProfilerMapPtr BuiltinFunctionCG = CreateBuiltinFunctionCG();

TConstAggregateProfilerMapPtr CreateBuiltinAggregateCG()
{
    auto result = New<TAggregateProfilerMap>();

    RegisterBuiltinFunctions(nullptr, nullptr, result.Get());

    return result;
}

const TConstAggregateProfilerMapPtr BuiltinAggregateCG = CreateBuiltinAggregateCG();

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
