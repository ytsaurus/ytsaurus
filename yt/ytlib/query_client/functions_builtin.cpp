#include "functions.h"
#include "functions_cg.h"
#include "cg_fragment_compiler.h"
#include "query.h"
#include "helpers.h"
#include "functions_builder.h"

#include "udf/is_prefix.h"
#include "udf/avg.h"
#include "udf/farm_hash.h"
#include "udf/hyperloglog.h"
#include "udf/is_substr.h"
#include "udf/lower.h"
#include "udf/concat.h"
#include "udf/max.h"
#include "udf/min.h"
#include "udf/regex.h"
#include "udf/sleep.h"
#include "udf/sum.h"
#include "udf/dates.h"

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
        const Stroka& name,
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
        const Stroka& name,
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
        const Stroka& name,
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
        const Stroka& name,
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
        TSharedRef(
            is_substr_bc,
            is_substr_bc_len,
            nullptr),
        ECallingConvention::Simple);

    builder.RegisterFunction(
        "lower",
        std::vector<TType>{EValueType::String},
        EValueType::String,
        TSharedRef(
            lower_bc,
            lower_bc_len,
            nullptr),
        ECallingConvention::Simple);

    builder.RegisterFunction(
        "concat",
        std::vector<TType>{EValueType::String, EValueType::String},
        EValueType::String,
        TSharedRef(
            concat_bc,
            concat_bc_len,
            nullptr),
        ECallingConvention::Simple);

    builder.RegisterFunction(
        "sleep",
        std::vector<TType>{EValueType::Int64},
        EValueType::Int64,
        TSharedRef(
            sleep_bc,
            sleep_bc_len,
            nullptr),
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
        TSharedRef(
            farm_hash_bc,
            farm_hash_bc_len,
            nullptr));

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

    builder.RegisterFunction(
        "regex_full_match",
        "regex_full_match",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::Boolean,
        TSharedRef(
            regex_bc,
            regex_bc_len,
            nullptr),
        New<TUnversionedValueCallingConvention>(-1, true));

    builder.RegisterFunction(
        "regex_partial_match",
        "regex_partial_match",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::Boolean,
        TSharedRef(
            regex_bc,
            regex_bc_len,
            nullptr),
        New<TUnversionedValueCallingConvention>(-1, true));

    builder.RegisterFunction(
        "regex_replace_first",
        "regex_replace_first",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::String,
        TSharedRef(
            regex_bc,
            regex_bc_len,
            nullptr),
        New<TUnversionedValueCallingConvention>(-1, true));

    builder.RegisterFunction(
        "regex_replace_all",
        "regex_replace_all",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::String,
        TSharedRef(
            regex_bc,
            regex_bc_len,
            nullptr),
        New<TUnversionedValueCallingConvention>(-1, true));

    builder.RegisterFunction(
        "regex_extract",
        "regex_extract",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{EValueType::String, EValueType::String, EValueType::String},
        EValueType::Null,
        EValueType::String,
        TSharedRef(
            regex_bc,
            regex_bc_len,
            nullptr),
        New<TUnversionedValueCallingConvention>(-1, true));

    builder.RegisterFunction(
        "regex_escape",
        "regex_escape",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{EValueType::String},
        EValueType::Null,
        EValueType::String,
        TSharedRef(
            regex_bc,
            regex_bc_len,
            nullptr),
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

    builder.RegisterAggregate(
        "sum",
        sumConstraints,
        typeArg,
        typeArg,
        typeArg,
        TSharedRef(
            sum_bc,
            sum_bc_len,
            nullptr),
        ECallingConvention::UnversionedValue);
    builder.RegisterAggregate(
        "min",
        constraints,
        typeArg,
        typeArg,
        typeArg,
        TSharedRef(
            min_bc,
            min_bc_len,
            nullptr),
        ECallingConvention::UnversionedValue);
    builder.RegisterAggregate(
        "max",
        constraints,
        typeArg,
        typeArg,
        typeArg,
        TSharedRef(
            max_bc,
            max_bc_len,
            nullptr),
        ECallingConvention::UnversionedValue);
    builder.RegisterAggregate(
        "avg",
        std::unordered_map<TTypeArgument, TUnionType>(),
        EValueType::Int64,
        EValueType::Double,
        EValueType::String,
        TSharedRef(
            avg_bc,
            avg_bc_len,
            nullptr),
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
        TSharedRef(
            hyperloglog_bc,
            hyperloglog_bc_len,
            nullptr),
        ECallingConvention::UnversionedValue);

    builder.RegisterFunction(
            "format_timestamp",
            std::vector<TType>{EValueType::Int64, EValueType::String},
            EValueType::String,
            TSharedRef(
                dates_bc,
                dates_bc_len,
                nullptr),
            ECallingConvention::Simple);

    std::vector<Stroka> timestampFloorFunctions = {
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
            TSharedRef(
                dates_bc,
                dates_bc_len,
                nullptr),
            ECallingConvention::Simple);
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
        TSharedRef(
            is_prefix_bc,
            is_prefix_bc_len,
            nullptr),
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
