
#include "folding_profiler.h"
#include "yt/yt/client/table_client/config.h"
#include "yt/yt/client/table_client/unversioned_row.h"
#include "yt/yt/client/table_client/unversioned_value.h"
#include <yt/yt/client/table_client/composite_compare.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/unversioned_writer.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/helpers.h>
#include <stdexcept>
#include <yt/yt/library/query/base/private.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/engine/simd_hashtable.h>
#include <yt/yt/library/query/engine_api/config.h>
#include <yt/yt/library/query/engine_api/evaluator.h>

#include <yt/yt/client/query_client/query_statistics.h>

#include <yt/yt/client/table_client/unversioned_writer.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/misc/async_slru_cache.h>
#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <llvm/ADT/FoldingSet.h>

#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/Threading.h>

#include <utility>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NProfiling;

using NCodegen::EExecutionBackend;

////////////////////////////////////////////////////////////////////////////////

struct TCachedCGQueryImage
    : public TAsyncCacheValueBase<llvm::FoldingSetNodeID, TCachedCGQueryImage>
{
    const TString Fingerprint;
    const TCGQueryImage Image;

    TCachedCGQueryImage(
        const llvm::FoldingSetNodeID& id,
        TString fingerprint,
        TCGQueryImage image)
        : TAsyncCacheValueBase(id)
        , Fingerprint(std::move(fingerprint))
        , Image(std::move(image))
    { }
};

using TCachedCGQueryImagePtr = TIntrusivePtr<TCachedCGQueryImage>;

void query_callback(TRange<TPIValue> r1, TRange<void *> r2, TRange<size_t> r3,
                    TExecutionContext *ctx) {
  std::ignore = r1;
  std::ignore = r2;
  std::ignore = r3;
  std::ignore = ctx;
  std::cout << "ricnorr, my query evaluator, r1.size() = " << r1.Size()
            << std::endl;
  TRowBatchReadOptions readOptions{.MaxRowsPerRead = 1024};
  IUnversionedRowBatchPtr batch = ctx->Reader->Read(readOptions);
  std::cout << "ricnorr, GetRowCount " << batch->GetRowCount() << std::endl;
  auto columnarBatch = batch->TryAsColumnar();
  std::cout << "ricnorr, columnarBatch is null? " << (columnarBatch == nullptr) << std::endl;
  if (columnarBatch == nullptr) {
    auto materializedRows = batch->MaterializeRows();
    std::cout << "ricnorr, columnarBatch is null!" << std::endl;
    for (const TUnversionedRow &row : materializedRows) {
      for (const TUnversionedValue &val : row) {
        std::cout << "ricnorr, val.type=" << static_cast<int>(val.Type) << std::endl;
      }
      std::cout << "ricnorr, row" << std::endl;
    }
  } else {
     std::cout << "it's columar batch" << std::endl;
    // throw std::runtime_error("it's columar batch:()");
  }
}

class TEvaluator
    : public TAsyncSlruCacheBase<llvm::FoldingSetNodeID, TCachedCGQueryImage>
    , public IEvaluator
{
    static void CheckQueryOptions(const TQueryBaseOptions& options)
    {
        THROW_ERROR_EXCEPTION_IF(options.InputRowLimit < 0, "Negative input row limit is forbidden");
        THROW_ERROR_EXCEPTION_IF(options.OutputRowLimit < 0, "Negative output row limit is forbidden");
    }

public:
    TEvaluator(
        const TExecutorConfigPtr& config,
        const NProfiling::TProfiler& profiler)
        : TAsyncSlruCacheBase(config->CGCache, profiler.WithPrefix("/cg_cache"))
    { }

    std::function<void(TRange<TPIValue> r1, TRange<void *> r2,
                       TRange<size_t> r3, TExecutionContext *ctx)>
    prepareInSimdQuery(const TConstBaseQueryPtr &query) {
      auto derivedQuery = dynamic_cast<const TQuery *>(query.Get());
      TConstExpressionPtr where_clause = derivedQuery->WhereClause;
      auto inExpr = derivedQuery->WhereClause->template As<TInExpression>();
      auto values_cnt = inExpr->Values.size();
      simd_hashtable_t simd_hashtable(__builtin_popcount(values_cnt * 2));
      for (const TRow &row : inExpr->Values) {
        assert(row.Elements().size() == 1);
        const TUnversionedValue &row_el = row.Elements()[0];
        assert(row_el.Type == EValueType::Int64);
        auto integralValue = row_el.Data.Int64;
        simd_hashtable.scalar_insert(static_cast<int32_t>(integralValue));
      }
      return [hashtable = std::move(simd_hashtable)](
                 TRange<TPIValue> r1, TRange<void *> r2, TRange<size_t> r3,
                 TExecutionContext *ctx) {
        std::ignore = r1;
        std::ignore = r2;
        std::ignore = r3;
        std::ignore = ctx;
        hashtable.scalar_find(1);
      };
    }

    bool isInSimdQuery(const TConstBaseQueryPtr &query) {
      auto derivedQuery = dynamic_cast<const TQuery *>(query.Get());
      if (derivedQuery != nullptr) {
        if (auto inExpr =
                derivedQuery->WhereClause->template As<TInExpression>()) {
          return inExpr->Simd;
          //   std::cout << "ricnorr, in_clause, simd = " << inExpr->Simd
          //             << std::endl;
          //   for (const TRow &row : inExpr->Values) {
          //     assert(row.Elements().size() == 1);
          //     const TUnversionedValue &row_el = row.Elements()[0];
          //     assert(row_el.Type == EValueType::Int64);
          //     auto integralValue = row_el.Data.Int64;
          //     std::ignore = integralValue;
          //     std::cout << integralValue << std::endl;
          //   }
        }
      }
      return false;
    }

    TQueryStatistics Run(
        const TConstBaseQueryPtr& query,
        const ISchemafulUnversionedReaderPtr& reader,
        const IUnversionedRowsetWriterPtr& writer,
        const TJoinSubqueryProfiler& joinProfiler,
        const TConstFunctionProfilerMapPtr& functionProfilers,
        const TConstAggregateProfilerMapPtr& aggregateProfilers,
        const IMemoryChunkProviderPtr& memoryChunkProvider,
        const TQueryBaseOptions& options,
        const TFeatureFlags& requestFeatureFlags,
        TFuture<TFeatureFlags> responseFeatureFlags) override
    {
        CheckQueryOptions(options);

        auto queryFingerprint = InferName(query, {.OmitValues = true});

        NTracing::TChildTraceContextGuard guard("QueryClient.Evaluate");
        NTracing::AnnotateTraceContext([&] (const auto& traceContext) {
            traceContext->AddTag("fragment_id", query->Id);
            traceContext->AddTag("query_fingerprint", queryFingerprint);
        });

        auto Logger = MakeQueryLogger(query);

        YT_LOG_DEBUG("Executing query (Fingerprint: %v, ReadSchema: %v, ResultSchema: %v, ExecutionBackend: %v)",
            queryFingerprint,
            *query->GetReadSchema(),
            *query->GetTableSchema(),
            options.ExecutionBackend);

        TQueryStatistics statistics;
        NProfiling::TWallTimer wallTime;
        NProfiling::TFiberWallTimer syncTime;

        auto finalLogger = Finally([&] {
            YT_LOG_DEBUG("Finalizing evaluation");
        });

        try {
            TCGVariables fragmentParams;
            auto queryInstance = Codegen(
                query,
                fragmentParams,
                joinProfiler,
                functionProfilers,
                aggregateProfilers,
                statistics,
                options.EnableCodeCache,
                options.UseCanonicalNullRelations,
                options.ExecutionBackend);

            // NB: Function contexts need to be destroyed before queryInstance since it hosts destructors.
            auto finalizer = Finally([&] {
                fragmentParams.Clear();
            });

            auto executionContext = TExecutionContext{
                .Reader = reader,
                .Writer = writer,
                .Statistics = &statistics,
                .InputRowLimit = options.InputRowLimit,
                .OutputRowLimit = options.OutputRowLimit,
                .GroupRowLimit = options.OutputRowLimit,
                .JoinRowLimit = options.OutputRowLimit,
                .Offset = query->Offset,
                .Limit = query->Limit,
                .Ordered = query->IsOrdered(),
                .IsMerge = dynamic_cast<const TFrontQuery*>(query.Get()) != nullptr,
                .MemoryChunkProvider = memoryChunkProvider,
                .RequestFeatureFlags = &requestFeatureFlags,
                .ResponseFeatureFlags = responseFeatureFlags,
            };

            YT_LOG_DEBUG("Evaluating query");
            std::cout << "ricnorr, Instantiated function (TCGQueryInstance "
                         "with big signature) is run"
                      << std::endl;
            [[maybe_unused]] TRange<TPIValue> literal_values = fragmentParams.GetLiteralValues();
            [[maybe_unused]] TRange<void *> opaque_data = fragmentParams.GetOpaqueData();
            [[maybe_unused]] TRange<size_t> opaque_data_sizes =
                fragmentParams.GetOpaqueDataSizes();
            std::cout << "ricnorr. Print literal values,size"
                      << literal_values.Size() << std::endl;
            for (size_t i = 0; i < literal_values.Size(); i++) {
              std::cout << "literal value:'" << literal_values[i].AsStringBuf()
                        << "'" << std::endl;
            }

            std::cout << "ricnorr. opaque data.size"
                      << opaque_data.Size() << std::endl;
            // for (size_t i = 0; i < opaque_data.Size(); i++) {
            //   std::cout << opaque_data[i].AsStringBuf() << std::endl;
            // }
            if (isInSimdQuery(query)) {
              query_callback(fragmentParams.GetLiteralValues(),
                             fragmentParams.GetOpaqueData(),
                             fragmentParams.GetOpaqueDataSizes(),
                             &executionContext);
            } else {
              prepareInSimdQuery(query)(fragmentParams.GetLiteralValues(),
                                        fragmentParams.GetOpaqueData(),
                                        fragmentParams.GetOpaqueDataSizes(),
                                        &executionContext);
            }
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Query evaluation failed");
            THROW_ERROR_EXCEPTION("Query evaluation failed") << ex;
        }

        statistics.SyncTime = syncTime.GetElapsedTime();
        statistics.AsyncTime = wallTime.GetElapsedTime() - statistics.SyncTime;
        statistics.ExecuteTime =
            statistics.SyncTime - statistics.ReadTime - statistics.WriteTime - statistics.CodegenTime;

        YT_LOG_DEBUG("Query statistics (%v)", statistics);

        // TODO(prime): place these into trace log
        //    TRACE_ANNOTATION("rows_read", statistics.RowsRead);
        //    TRACE_ANNOTATION("rows_written", statistics.RowsWritten);
        //    TRACE_ANNOTATION("sync_time", statistics.SyncTime);
        //    TRACE_ANNOTATION("async_time", statistics.AsyncTime);
        //    TRACE_ANNOTATION("execute_time", statistics.ExecuteTime);
        //    TRACE_ANNOTATION("read_time", statistics.ReadTime);
        //    TRACE_ANNOTATION("write_time", statistics.WriteTime);
        //    TRACE_ANNOTATION("codegen_time", statistics.CodegenTime);
        //    TRACE_ANNOTATION("incomplete_input", statistics.IncompleteInput);
        //    TRACE_ANNOTATION("incomplete_output", statistics.IncompleteOutput);

        return statistics;
    }

private:
    TCGQueryInstance Codegen(
        TConstBaseQueryPtr query,
        TCGVariables& variables,
        const TJoinSubqueryProfiler& joinProfiler,
        const TConstFunctionProfilerMapPtr& functionProfilers,
        const TConstAggregateProfilerMapPtr& aggregateProfilers,
        TQueryStatistics& statistics,
        bool enableCodeCache,
        bool useCanonicalNullRelations,
        EExecutionBackend executionBackend)
    {
        llvm::FoldingSetNodeID id;

        // функция void -> CogegenImage
        auto makeCodegenQuery = Profile(
            query,
            &id,
            &variables,
            joinProfiler,
            useCanonicalNullRelations,
            executionBackend,
            functionProfilers,
            aggregateProfilers);

        auto Logger = MakeQueryLogger(query);

        // See condition in folding_profiler.cpp.
        bool considerLimit = query->IsOrdered() && !query->GroupClause;

        auto queryFingerprint = InferName(query, TInferNameOptions{
            .OmitValues=true,
            .OmitAliases=true,
            .OmitJoinPredicate=true,
            .OmitOffsetAndLimit=!considerLimit,
        });
        auto compileWithLogging = [&] {
            NTracing::TChildTraceContextGuard traceContextGuard("QueryClient.Compile");
            YT_LOG_DEBUG("Started compiling fragment");
            TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(&statistics.CodegenTime);
            auto image = makeCodegenQuery();
            std::cout << "ricnorr: image can be instantiated to fun with sign (TRange<TPIValue>, TRange<void*>, TRange<size_t>, TExecutionContext*, NWebAssembly::IWebAssemblyCompartment*)";
            auto cachedImage = New<TCachedCGQueryImage>(id, queryFingerprint, std::move(image));
            YT_LOG_DEBUG("Finished compiling fragment");
            return cachedImage;
        };

        TCachedCGQueryImagePtr cachedQueryImage;
        if (enableCodeCache) {
            auto cookie = BeginInsert(id);
            if (cookie.IsActive()) {
                YT_LOG_DEBUG("Codegen cache miss: generating query evaluator");

                try {
                    cookie.EndInsert(compileWithLogging());
                } catch (const std::exception& ex) {
                    YT_LOG_DEBUG(ex, "Failed to compile a query fragment");
                    cookie.Cancel(TError(ex).Wrap("Failed to compile a query fragment"));
                }
            }

            cachedQueryImage = WaitForFast(cookie.GetValue())
                .ValueOrThrow();

            // Query fingerprints can differ when folding ids are equal in the following case:
            // WHERE predicate is split into multiple predicates which are evaluated before join and after it.
            // Example:
            // from [a] a join [b] b where a.k and b.k
            // from [a] a join [b] b where b.k and a.k
        } else {
            YT_LOG_DEBUG("Codegen cache disabled");

            cachedQueryImage = compileWithLogging();
        }

        NTracing::TChildTraceContextGuard traceContextGuard("QueryClient.Compile");
        TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(&statistics.CodegenTime);
        return cachedQueryImage->Image.Instantiate();
    }
};

////////////////////////////////////////////////////////////////////////////////

IEvaluatorPtr CreateEvaluator(TExecutorConfigPtr config, const NProfiling::TProfiler& profiler)
{
    return New<TEvaluator>(std::move(config), profiler);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
