#include "ql_helpers.h"

#include <yt/yt/library/query/engine/folding_profiler.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/core/yson/public.h>
#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NQueryClient {

using NCodegen::EExecutionBackend;

////////////////////////////////////////////////////////////////////////////////

void PrintTo(TConstExpressionPtr expr, ::std::ostream* os)
{
    *os << InferName(expr);
}

TValue MakeInt64(i64 value)
{
    return MakeUnversionedInt64Value(value);
}

TValue MakeUint64(ui64 value)
{
    return MakeUnversionedUint64Value(value);
}

TValue MakeDouble(double value)
{
    return MakeUnversionedDoubleValue(value);
}

TValue MakeBoolean(bool value)
{
    return MakeUnversionedBooleanValue(value);
}

TValue MakeString(TStringBuf value)
{
    return MakeUnversionedStringValue(value);
}

TValue MakeNull()
{
    return MakeUnversionedSentinelValue(EValueType::Null);
}

TValue MakeAny(TStringBuf ysonString)
{
    return MakeUnversionedAnyValue(ysonString);
}

TValue MakeComposite(TStringBuf ysonString)
{
    return MakeUnversionedCompositeValue(ysonString);
}

TTableSchemaPtr GetSampleTableSchema()
{
    return New<TTableSchema>(std::vector{
        TColumnSchema("k", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("l", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("m", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64),
        TColumnSchema("b", EValueType::Int64),
        TColumnSchema("c", EValueType::Int64),
        TColumnSchema("s", EValueType::String),
        TColumnSchema("u", EValueType::String),
        TColumnSchema("ki", EValueType::Int64),
        TColumnSchema("ku", EValueType::Uint64),
        TColumnSchema("kd", EValueType::Double),
        TColumnSchema("any_key", EValueType::Any),
    });
}

TDataSplit MakeSimpleSplit()
{
    TDataSplit dataSplit;
    dataSplit.TableSchema = GetSampleTableSchema();
    return dataSplit;
}

TDataSplit MakeSplit(const std::vector<TColumnSchema>& columns)
{
    TDataSplit dataSplit;
    dataSplit.ObjectId = TGuid::Create();
    dataSplit.TableSchema = New<TTableSchema>(columns);
    return dataSplit;
}

TFuture<TDataSplit> RaiseTableNotFound(const TYPath& path)
{
    return MakeFuture<TDataSplit>(TError(
        "Could not find table %v",
        path));
}

////////////////////////////////////////////////////////////////////////////////

void ProfileForBothExecutionBackends(
    const TConstBaseQueryPtr& query,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables,
    const std::vector<IJoinProfilerPtr>& joinProfilers)
{
    Profile(query, id, variables, joinProfilers, /*useCanonicalNullRelations*/ false, EExecutionBackend::Native)();
    if (EnableWebAssemblyInUnitTests()) {
        Profile(query, id, variables, joinProfilers, /*useCanonicalNullRelations*/ false, EExecutionBackend::WebAssembly)();
    }
}

void ProfileForBothExecutionBackends(
    const TConstExpressionPtr& expr,
    const TTableSchemaPtr& schema,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables)
{
    Profile(expr, schema, id, variables, /*useCanonicalNullRelations*/ false, EExecutionBackend::Native)();
    if (EnableWebAssemblyInUnitTests()) {
        Profile(expr, schema, id, variables, /*useCanonicalNullRelations*/ false, EExecutionBackend::WebAssembly)();
    }
}

////////////////////////////////////////////////////////////////////////////////

IJoinProfilerPtr MakeNullJoinSubqueryProfiler()
{
    class TNullJoinSubqueryProfiler
        : public IJoinProfiler
    {
    public:
        TNullJoinSubqueryProfiler() = default;

        IJoinRowsProducerPtr Profile() override
        {
            return nullptr;
        }
    };

    return New<TNullJoinSubqueryProfiler>();
}

////////////////////////////////////////////////////////////////////////////////

int DefaultExpressionBuilderVersion = 1;

TPlanFragmentPtr ParseAndPreparePlanFragment(
    IPrepareCallbacks* callbacks,
    TStringBuf source,
    NYson::TYsonStringBuf placeholderValues,
    int syntaxVersion,
    IMemoryUsageTrackerPtr memoryTracker)
{
    auto parsedSource = ParseSource(source, EParseMode::Query, placeholderValues, syntaxVersion);
    return PreparePlanFragment(
        callbacks,
        parsedSource->Source,
        std::get<NAst::TQuery>(parsedSource->AstHead.Ast),
        parsedSource->AstHead.AliasMap,
        DefaultExpressionBuilderVersion,
        std::move(memoryTracker));
}

TConstExpressionPtr ParseAndPrepareExpression(
    TStringBuf source,
    const TTableSchema& tableSchema,
    const TConstTypeInferrerMapPtr& functions,
    THashSet<std::string>* references)
{
    return PrepareExpression(
        *ParseSource(source, EParseMode::Expression),
        tableSchema,
        DefaultExpressionBuilderVersion,
        functions,
        references);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
