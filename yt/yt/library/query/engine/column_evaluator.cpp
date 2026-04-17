
#include "cg_fragment_compiler.h"
#include "folding_profiler.h"
#include "functions_cg.h"

#include <yt/yt/library/query/base/query_preparer.h>
#include <yt/yt/library/query/base/functions.h>
#include <yt/yt/library/query/base/private.h>

#include <yt/yt/library/query/engine_api/builtin_function_profiler.h>
#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>

#include <yt/yt/client/query_client/query_statistics.h>

#include <yt/yt/core/misc/sync_cache.h>

namespace NYT::NQueryClient {

using namespace NTableClient;
using namespace NYTree;

using NCodegen::EExecutionBackend;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

void Init(const TRowBufferPtr&, TValue* value, NWebAssembly::IWebAssemblyCompartment*)
{
    *value = MakeUnversionedNullValue();
}

template <class T, class Value = TValue>
T& GetTypedData(Value* value)
{
    if constexpr (std::is_same_v<std::decay_t<T>, i64>) {
        return value->Data.Int64;
    } else if constexpr (std::is_same_v<std::decay_t<T>, ui64>) {
        return value->Data.Uint64;
    } else if constexpr (std::is_same_v<std::decay_t<T>, double>) {
        return value->Data.Double;
    } else {
        static_assert(false, "Unimplemented builtin aggregate");
    }
}

template <class T, T Aggregate(T, T)>
void Update(const TRowBufferPtr&, TValue* state, TRange<TValue> incomingValues, NWebAssembly::IWebAssemblyCompartment*)
{
    for (auto value : incomingValues) {
        if (value.Type != EValueType::Null) {
            if (state->Type == EValueType::Null) {
                *state = value;
            } else {
                GetTypedData<T>(state) = Aggregate(GetTypedData<T>(state), GetTypedData<T>(&value));
            }
        }
    }
}

template <class T, T Aggregate(T, T)>
void Merge(const TRowBufferPtr&, TValue* updatedState, const TValue* incomingState, NWebAssembly::IWebAssemblyCompartment*)
{
    if (incomingState->Type == EValueType::Null) {
        return;
    }

    if (updatedState->Type == EValueType::Null) {
        *updatedState = *incomingState;
    } else {
        GetTypedData<T>(updatedState) = Aggregate(GetTypedData<T>(updatedState), GetTypedData<const T, const TValue>(incomingState));
    }
}

void Finalize(const TRowBufferPtr&, TValue* value, const TValue* state, NWebAssembly::IWebAssemblyCompartment*)
{
    *value = *state;
}

TCGAggregateImage MakeBuiltinAggregate(const std::string& name, EValueType wireType)
{
    TCGAggregateCallbacks callbacks;
    callbacks.Init = BIND(Init);
    callbacks.Finalize = BIND(Finalize);

    if (name == "sum") {
        switch (wireType) {
            case EValueType::Int64:
                callbacks.Update = BIND(Update<i64, [] (i64 l, i64 r) { return l + r; }>);
                callbacks.Merge = BIND(Merge<i64, [] (i64 l, i64 r) { return l + r; }>);
                break;

            case EValueType::Uint64:
                callbacks.Update = BIND(Update<ui64, [] (ui64 l, ui64 r) { return l + r; }>);
                callbacks.Merge = BIND(Merge<ui64, [] (ui64 l, ui64 r) { return l + r; }>);
                break;

            case EValueType::Double:
                callbacks.Update = BIND(Update<double, [] (double l, double r) { return l + r; }>);
                callbacks.Merge = BIND(Merge<double, [] (double l, double r) { return l + r; }>);
                break;

            default:
                YT_ABORT();
        }
    } else if (name == "min") {
        switch (wireType) {
            case EValueType::Int64:
                callbacks.Update = BIND(Update<i64, [] (i64 l, i64 r) { return std::min(l, r); }>);
                callbacks.Merge = BIND(Merge<i64, [] (i64 l, i64 r) { return std::min(l, r); }>);
                break;

            case EValueType::Uint64:
                callbacks.Update = BIND(Update<ui64, [] (ui64 l, ui64 r) { return std::min(l, r); }>);
                callbacks.Merge = BIND(Merge<ui64, [] (ui64 l, ui64 r) { return std::min(l, r); }>);
                break;

            case EValueType::Double:
                callbacks.Update = BIND(Update<double, [] (double l, double r) { return std::min(l, r); }>);
                callbacks.Merge = BIND(Merge<double, [] (double l, double r) { return std::min(l, r); }>);
                break;

            default:
                YT_ABORT();
        }

    } else if (name == "max") {
        switch (wireType) {
            case EValueType::Int64:
                callbacks.Update = BIND(Update<i64, [] (i64 l, i64 r) { return std::max(l, r); }>);
                callbacks.Merge = BIND(Merge<i64, [] (i64 l, i64 r) { return std::max(l, r); }>);
                break;

            case EValueType::Uint64:
                callbacks.Update = BIND(Update<ui64, [] (ui64 l, ui64 r) { return std::max(l, r); }>);
                callbacks.Merge = BIND(Merge<ui64, [] (ui64 l, ui64 r) { return std::max(l, r); }>);
                break;

            case EValueType::Double:
                callbacks.Update = BIND(Update<double, [] (double l, double r) { return std::max(l, r); }>);
                callbacks.Merge = BIND(Merge<double, [] (double l, double r) { return std::max(l, r); }>);
                break;

            default:
                YT_ABORT();
        }
    } else {
        YT_ABORT();
    }

    return {std::move(callbacks), /*compartment*/ nullptr};
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

// TODO(dtorilov): Consider enabling WebAssembly for column evaluators.

TColumnEvaluatorPtr TColumnEvaluator::Create(
    const TTableSchemaPtr& schema,
    const TConstTypeInferrerMapPtr& typeInferrers,
    const TConstFunctionProfilerMapPtr& profilers)
{
    std::vector<TColumn> columns(schema->GetColumnCount());
    std::vector<bool> isAggregate(schema->GetColumnCount());

    for (int index = 0; index < schema->GetColumnCount(); ++index) {
        THashSet<std::string> references;
        auto& column = columns[index];

        if (schema->Columns()[index].Expression()) {
            column.Expression = PrepareExpression(
                *schema->Columns()[index].Expression(),
                *schema,
                typeInferrers,
                &references);

            column.EvaluatorImage = Profile(
                column.Expression,
                schema,
                /*id*/ nullptr,
                &column.Variables,
                /*useCanonicalNullRelations*/ false,
                EExecutionBackend::Native,
                profilers)();

            column.EvaluatorInstance = column.EvaluatorImage.Instantiate();

            for (const auto& reference : references) {
                column.ReferenceIds.push_back(schema->GetColumnIndexOrThrow(reference));
            }
            std::sort(column.ReferenceIds.begin(), column.ReferenceIds.end());
        }

        if (schema->Columns()[index].Aggregate()) {
            const auto& aggregateName = *schema->Columns()[index].Aggregate();
            if (TryParseNestedAggregate(aggregateName)) {
                continue;
            }
            auto type = schema->Columns()[index].LogicalType();
            auto wireType = GetWireType(type);
            if ((aggregateName == "sum" || aggregateName == "min" || aggregateName == "max") &&
                IsArithmeticType(wireType))
            {
                column.AggregateImage = MakeBuiltinAggregate(aggregateName, wireType);
            } else {
                column.AggregateImage = CodegenAggregate(
                    GetBuiltinAggregateProfilers()->GetAggregate(aggregateName)->Profile(
                        {type},
                        type,
                        type,
                        aggregateName,
                        EExecutionBackend::Native),
                    {wireType},
                    wireType,
                    EExecutionBackend::Native,
                    NWebAssembly::GetBuiltinSdk(),
                    {});
            }
            column.AggregateInstance = column.AggregateImage.Instantiate();
            isAggregate[index] = true;
        }

        // Perform lazy initialization to make Variables immutable and thread-safe in the future.
        column.Variables.GetLiteralValues();
    }

    return New<TColumnEvaluator>(std::move(columns), std::move(isAggregate));
}

////////////////////////////////////////////////////////////////////////////////

class TCachedColumnEvaluator
    : public TSyncCacheValueBase<llvm::FoldingSetNodeID, TCachedColumnEvaluator>
{
public:
    TCachedColumnEvaluator(
        const llvm::FoldingSetNodeID& id,
        TColumnEvaluatorPtr evaluator)
        : TSyncCacheValueBase(id)
        , Evaluator_(std::move(evaluator))
    { }

    const TColumnEvaluatorPtr& GetColumnEvaluator()
    {
        return Evaluator_;
    }

private:
    const TColumnEvaluatorPtr Evaluator_;
};

// TODO(lukyan): Use async cache?
class TColumnEvaluatorCache
    : public TSyncSlruCacheBase<llvm::FoldingSetNodeID, TCachedColumnEvaluator>
    , public IColumnEvaluatorCache
{
public:
    TColumnEvaluatorCache(
        TColumnEvaluatorCacheConfigPtr config,
        const TConstTypeInferrerMapPtr& typeInferrers,
        const TConstFunctionProfilerMapPtr& profilers)
        : TSyncSlruCacheBase(config->CGCache)
        , TypeInferrers_(typeInferrers)
        , Profilers_(profilers)
    { }

    TColumnEvaluatorPtr Find(const TTableSchemaPtr& schema) override
    {
        llvm::FoldingSetNodeID id;
        Profile(schema, &id);

        auto cachedEvaluator = TSyncSlruCacheBase::Find(id);
        if (!cachedEvaluator) {
            YT_LOG_DEBUG("Codegen cache miss: generating column evaluator (Schema: %v)",
                *schema);

            auto evaluator = TColumnEvaluator::Create(
                schema,
                TypeInferrers_,
                Profilers_);
            cachedEvaluator = New<TCachedColumnEvaluator>(id, evaluator);

            TryInsert(cachedEvaluator, &cachedEvaluator);
        }

        return cachedEvaluator->GetColumnEvaluator();
    }

    void Configure(const TColumnEvaluatorCacheDynamicConfigPtr& config) override
    {
        TSyncSlruCacheBase::Reconfigure(config->CGCache);
    }

private:
    const TConstTypeInferrerMapPtr TypeInferrers_;
    const TConstFunctionProfilerMapPtr Profilers_;
};

IColumnEvaluatorCachePtr CreateColumnEvaluatorCache(
    TColumnEvaluatorCacheConfigPtr config,
    TConstTypeInferrerMapPtr typeInferrers,
    TConstFunctionProfilerMapPtr profilers)
{
    return New<TColumnEvaluatorCache>(
        std::move(config),
        std::move(typeInferrers),
        std::move(profilers));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
