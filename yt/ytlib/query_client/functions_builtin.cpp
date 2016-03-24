#include "functions.h"
#include "functions_cg.h"
#include "cg_fragment_compiler.h"
#include "plan_fragment.h"
#include "helpers.h"
#include "functions_builder.h"

#include "udf/is_prefix.h"
#include "udf/avg.h"
#include "udf/double_cast.h"
#include "udf/farm_hash.h"
#include "udf/hyperloglog.h"
#include "udf/int64.h"
#include "udf/is_null.h"
#include "udf/is_substr.h"
#include "udf/lower.h"
#include "udf/concat.h"
#include "udf/max.h"
#include "udf/min.h"
#include "udf/regex.h"
#include "udf/sleep.h"
#include "udf/sum.h"
#include "udf/uint64.h"
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
        TCGContext& builder,
        const std::vector<TCodegenExpression>& codegenArgs,
        EValueType type,
        const Stroka& name,
        Value* row)
    {
        auto nameTwine = Twine(name.c_str());

        YCHECK(codegenArgs.size() == 3);
        auto condition = codegenArgs[0](builder, row);
        YCHECK(condition.GetStaticType() == EValueType::Boolean);

        return CodegenIf<TCGContext, TCGValue>(
            builder,
            condition.IsNull(),
            [&] (TCGContext& builder) {
                return TCGValue::CreateNull(builder, type);
            },
            [&] (TCGContext& builder) {
                return CodegenIf<TCGContext, TCGValue>(
                    builder,
                    builder.CreateICmpNE(
                        builder.CreateZExtOrBitCast(condition.GetData(), builder.getInt64Ty()),
                        builder.getInt64(0)),
                    [&] (TCGContext& builder) {
                        return codegenArgs[1](builder, row);
                    },
                    [&] (TCGContext& builder) {
                        return codegenArgs[2](builder, row);
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
        ] (TCGContext& builder, Value* row) {
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

} // namespace NBuiltins

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

    builder.RegisterFunction(
        "is_null",
        std::vector<TType>{0},
        EValueType::Boolean,
        TSharedRef(
            is_null_bc,
            is_null_bc_len,
            nullptr),
        ECallingConvention::UnversionedValue);

    auto typeArg = 0;
    auto castConstraints = std::unordered_map<TTypeArgument, TUnionType>();
    castConstraints[typeArg] = std::vector<EValueType>{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double};

    builder.RegisterFunction(
        "int64",
        castConstraints,
        std::vector<TType>{typeArg},
        EValueType::Null,
        EValueType::Int64,
        TSharedRef(
            int64_bc,
            int64_bc_len,
            nullptr));

    builder.RegisterFunction(
        "uint64",
        castConstraints,
        std::vector<TType>{typeArg},
        EValueType::Null,
        EValueType::Uint64,
        TSharedRef(
            uint64_bc,
            uint64_bc_len,
            nullptr));

    builder.RegisterFunction(
        "double",
        "double_cast",
        castConstraints,
        std::vector<TType>{typeArg},
        EValueType::Null,
        EValueType::Double,
        TSharedRef(
            double_cast_bc,
            double_cast_bc_len,
            nullptr));

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
