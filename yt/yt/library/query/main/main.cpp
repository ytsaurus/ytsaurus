#include "yt/yt/client/table_client/config.h"
#include "yt/yt/client/table_client/helpers.h"
#include "yt/yt/core/concurrency/scheduler_api.h"
#include "yt/yt/library/query/engine/cg_fragment_compiler.h"
#include "yt/yt/library/query/engine_api/evaluation_helpers.h"
#include "yt/yt/client/table_client/row_batch.h"
#include "yt/yt/client/table_client/schema.h"
#include "yt/yt/client/query_client/query_statistics.h"
#include "yt/yt/client/table_client/pipe.h"
#include "yt/yt/client/table_client/unversioned_reader.h"
#include "yt/yt/client/table_client/unversioned_row.h"
#include "yt/yt/core/actions/future.h"
#include "yt/yt/library/query/base/ast.h"
#include "yt/yt/library/query/base/ast_visitors.h"
#include "yt/yt/library/query/base/public.h"
#include "yt/yt/library/query/base/query.h"
#include "yt/yt/library/query/base/query_preparer.h"
#include "yt/yt/library/query/engine_api/config.h"
#include "yt/yt/library/query/engine_api/evaluator.h"
#include "llvm/ADT/FoldingSet.h"

using namespace NYT;
using namespace NYT::NQueryClient;

TDataSplit MakeSplit(const std::vector<TColumnSchema> &columns) {
  TDataSplit dataSplit;
  dataSplit.ObjectId = TGuid::Create();
  dataSplit.TableSchema = New<TTableSchema>(columns);
  return dataSplit;
}

std::vector<TColumnSchema> columns = {{{"a", EValueType::Int64}}};
TDataSplit dataSplit = MakeSplit(columns);

struct MyPrepareCallback : public IPrepareCallbacks {
  TFuture<TDataSplit> GetInitialSplit(const NYPath::TYPath &) {
    return MakeFuture(dataSplit);
  }
};

TQueryPtr Prepare(const TString &query) {
  MyPrepareCallback callback;
  auto fragment = NYT::NQueryClient::PreparePlanFragment(&callback, query);

  return fragment->Query;
}

TCGQueryInstance
Codegen(TConstBaseQueryPtr /*query*/) {
  // llvm::FoldingSetNodeID id;

  // auto makeCodegenQuery =
  //     Profile(query, &id, &variables, joinProfiler, useCanonicalNullRelations,
  //             executionBackend, functionProfilers, aggregateProfilers);
  // See condition in folding_profiler.cpp.
  // bool considerLimit = query->IsOrdered() && !query->GroupClause;

  // auto queryFingerprint =
  //     InferName(query, TInferNameOptions{
  //                          .OmitValues = true,
  //                          .OmitAliases = true,
  //                          .OmitJoinPredicate = true,
  //                          .OmitOffsetAndLimit = !considerLimit,
  //                      });
  // auto compileWithLogging = [&] {
  //   NTracing::TChildTraceContextGuard traceContextGuard("QueryClient.Compile");
  //   YT_LOG_DEBUG("Started compiling fragment");
  //   TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(
  //       &statistics.CodegenTime);
  //   auto image = makeCodegenQuery();
  //   auto cachedImage =
  //       New<TCachedCGQueryImage>(id, queryFingerprint, std::move(image));
  //   YT_LOG_DEBUG("Finished compiling fragment");
  //   return cachedImage;
  // };
  TCodegenSource codegenSource = [](TCGOperatorContext& /*builder*/) {
    
  };
  size_t slotCount = 0;
  TCGQueryImage image =
      CodegenQuery(&codegenSource, /*size_t slotCount*/ slotCount,
                   NYT::NCodegen::EExecutionBackend::Native);
  return image.Instantiate();
  // auto image = makeCodegenQuery();  
  // TCachedCGQueryImagePtr cachedQueryImage = compileWithLogging();
  // NTracing::TChildTraceContextGuard traceContextGuard("QueryClient.Compile");
  // TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(
  //     &statistics.CodegenTime);
  // return cachedQueryImage->Image.Instantiate();
}


void PlayWithCodegen() {
  TCodegenSource codegenSource;
}

int main() {
  std::vector<TColumnSchema> columns;
  columns.push_back({"a", EValueType::Int64});
  std::map<TString, TDataSplit> splits;
  splits["//t"] = MakeSplit(columns);

  const IEvaluatorPtr Evaluator_ =
      CreateEvaluator(NYT::New<NYT::NQueryClient::TExecutorConfig>());
  // ricnorr, query
  TString query = "a from t where a in_simd (0, 1, 2, 3, 4, 5, 6, 7, 9, 10)";
  std::unique_ptr<TParsedSource> parsed_source =
      ParseSource(query, EParseMode::Query, {}, 1);
  NAst::TAliasMap aliasMap = parsed_source->AstHead.AliasMap;
  NAst::TQuery ast = std::get<NAst::TQuery>(parsed_source->AstHead.Ast);
  //   const auto &table = ast.Table;
  // std::vector<TFuture<TDataSplit>> asyncDataSplits;
  // asyncDataSplits.reserve(ast.Joins.size() + 1);
  // asyncDataSplits.push_back(callbacks->GetInitialSplit(table.Path));
  NYT::NQueryClient::NAst::TTableDescriptor table = ast.Table;
  std::cout << "table path : " << table.Path << std::endl;
  if (table.Alias.has_value()) {
    std::cout << "table alias : " << table.Alias.value() << std::endl;
  } else {
    std::cout << "table alias : " << "no alias" << std::endl;
  }

  NAst::TNullableExpressionList maybeWhereList = ast.WherePredicate;
  if (maybeWhereList.has_value()) {
    NAst::TExpressionList whereList = maybeWhereList.value();
    // using TExpressionList = std::vector<TExpressionPtr>;
    std::cout << "where size = " << whereList.size() << std::endl;
    // NYT::NQueryClient::NAst::TAstVisitor ast_visitor;
    NYT::NQueryClient::TColumnSet columnSet;
    for (NYT::NQueryClient::NAst::TExpression *whereEl : whereList) {
      NYT::NQueryClient::NAst::TReferenceHarvester(&columnSet).Visit(whereEl);
      // std::ignore = whereEl;
      // std::cout << "where el: " << std::endl;
    }
    for (const TString &column : columnSet) {
      std::cout << "where column " << column << std::endl;
    }
  } else {
    std::cout << "no where " << std::endl;
  }

  for (const auto &join : ast.Joins) {
    std::ignore = join;
    std::cout << "join iteration";
    //   Visit(
    //       join,
    //       [&](const NAst::TJoin &tableJoin) {
    //         asyncDataSplits.push_back(
    //             callbacks->GetInitialSplit(tableJoin.Table.Path));
    //       },
    //       [&](const NAst::TArrayJoin & /*arrayJoin*/) {
    //         auto defaultPromise = NewPromise<TDataSplit>();
    //         defaultPromise.Set(TDataSplit{});
    //         asyncDataSplits.push_back(defaultPromise.ToFuture());
    //       });
  }
  //   const auto &ast = std::get<NAst::TQuery>(astHead.Ast);
  //   const auto &aliasMap = astHead.AliasMap;

  //   std::cout << "source " << parsed_source.get()->AstHead << "\n";
  //   ParseSource(source, EParseMode::Query, placeholderValues,
  //   syntaxVersion) const TConstBaseQueryPtr Query_ = Prepare(
  //     query,
  //     splits,
  //     {},
  //     0
  //   );


  struct MyReader : NTableClient::ISchemafulUnversionedReader {

    bool isFirstRead = true;
    // Use small batch size for tests.
    const size_t maxBatchSize = 5;

    ssize_t batchSize = maxBatchSize;

    NTableClient::TUnversionedRow *rowsBegin;
    NTableClient::TUnversionedRow *rowsEnd;
    std::vector<TOwningRow> owningSourceRows;
    std::vector<TRow> sourceRows;
    TQueryPtr query;

    MyReader(TQueryPtr query, std::vector<TString> source) {
      this->query = query;
      for (const auto &s : source) {
        owningSourceRows.push_back(
            NTableClient::YsonToSchemafulRow(s, *query->GetReadSchema(), true));
      }

      for (const auto &row : owningSourceRows) {
        sourceRows.push_back(row.Get());
      }

      rowsBegin = sourceRows.begin();
      rowsEnd = sourceRows.end();

      isFirstRead = true;
    }

    NYT::NTableClient::IUnversionedRowBatchPtr
    ReadRows(const NYT::NTableClient::TRowBatchReadOptions &options = {}) {
      auto readCount = std::distance(sourceRows.begin(), rowsBegin);
      for (ssize_t index = 0; index < readCount; ++index) {
        sourceRows[index] = TRow();
        owningSourceRows[index] = TOwningRow();
      }

      if (isFirstRead && query->IsOrdered()) {
        isFirstRead = false;
      }

      auto size = std::min<size_t>(options.MaxRowsPerRead,
                                   std::distance(rowsBegin, rowsEnd));
      std::vector<TRow> rows(rowsBegin, rowsBegin + size);
      rowsBegin += size;
      batchSize = std::min<size_t>(batchSize * 2, maxBatchSize);
      return rows.empty() ? nullptr
                          : CreateBatchFromUnversionedRows(
                                MakeSharedRange(std::move(rows)));
    }

  //  NYT::NProto::TDataStatistics GetDataStatistics() const override
  //   {
  //       return {};
  //   }

    NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        return {};
    }

    bool IsFetchingCompleted() const override
    {
        return false;
    }

    std::vector<NYT::NChunkClient::TChunkId> GetFailedChunkIds() const override
    {
        return {};
    }

    NYT::NChunkClient::NProto::TDataStatistics GetDataStatistics() const override {
      return {};
    }

    void DestroyRefCounted() override {}

    TFuture<void> GetReadyEvent() const override { return VoidFuture; }

    NYT::NTableClient::IUnversionedRowBatchPtr
    Read(const NYT::NTableClient::TRowBatchReadOptions &options = {}) override {
      return ReadRows(options);
    }
  };


  struct MyColumnReader : NTableClient::ISchemafulUnversionedReader {

    bool isFirstRead = true;
    // Use small batch size for tests.
    const size_t maxBatchSize = 5;

    ssize_t batchSize = maxBatchSize;

    NTableClient::TUnversionedRow *rowsBegin;
    NTableClient::TUnversionedRow *rowsEnd;
    std::vector<TOwningRow> owningSourceRows;
    std::vector<TRow> sourceRows;
    TQueryPtr query;

    MyColumnReader(TQueryPtr query, std::vector<TString> source) {
      this->query = query;
      for (const auto &s : source) {
        owningSourceRows.push_back(
            NTableClient::YsonToSchemafulRow(s, *query->GetReadSchema(), true));
      }

      for (const auto &row : owningSourceRows) {
        sourceRows.push_back(row.Get());
      }

      rowsBegin = sourceRows.begin();
      rowsEnd = sourceRows.end();

      isFirstRead = true;
    }

    NYT::NTableClient::IUnversionedRowBatchPtr
    ReadRows(const NYT::NTableClient::TRowBatchReadOptions &options = {}) {
      auto readCount = std::distance(sourceRows.begin(), rowsBegin);
      for (ssize_t index = 0; index < readCount; ++index) {
        sourceRows[index] = TRow();
        owningSourceRows[index] = TOwningRow();
      }

      if (isFirstRead && query->IsOrdered()) {
        isFirstRead = false;
      }

      auto size = std::min<size_t>(options.MaxRowsPerRead,
                                   std::distance(rowsBegin, rowsEnd));
      std::vector<TRow> rows(rowsBegin, rowsBegin + size);
      rowsBegin += size;
      batchSize = std::min<size_t>(batchSize * 2, maxBatchSize);
      return rows.empty() ? nullptr
                          : CreateBatchFromUnversionedRows(
                                MakeSharedRange(std::move(rows)));
    }

  //  NYT::NProto::TDataStatistics GetDataStatistics() const override
  //   {
  //       return {};
  //   }

    NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        return {};
    }

    bool IsFetchingCompleted() const override
    {
        return false;
    }

    std::vector<NYT::NChunkClient::TChunkId> GetFailedChunkIds() const override
    {
        return {};
    }

    NYT::NChunkClient::NProto::TDataStatistics GetDataStatistics() const override {
      return {};
    }

    void DestroyRefCounted() override {}

    TFuture<void> GetReadyEvent() const override { return VoidFuture; }

    NYT::NTableClient::IUnversionedRowBatchPtr
    Read(const NYT::NTableClient::TRowBatchReadOptions &options = {}) override {
      return ReadRows(options);
    }
  };



  auto query_ptr = Prepare(query);

  std::vector<TString> source = {"a=0", "a=2"};

  ISchemafulUnversionedReaderPtr reader = New<MyReader>(query_ptr, source);
  // auto reader = MyReader(query_ptr, source);

  // auto pipe = New<NYT::NTableClient::TSchemafulPipe>(GetDefaultMemoryChunkProvider());

  // TCodegenSource codegenSource = &CodegenEmptyOp;


  // TCGQueryImage image;
  // image.Callback_();
  IUnversionedRowsetWriterPtr writer;
  // using TCGQuerySignature = void(TRange<TPIValue>, TRange<void*>, TRange<size_t>, TExecutionContext*, NWebAssembly::IWebAssemblyCompartment*);
  TFuture<NYT::NApi::IUnversionedRowsetPtr> asyncResultRowset;
  std::tie(writer, asyncResultRowset) = NYT::NApi::CreateSchemafulRowsetWriter(query_ptr->GetTableSchema());
   Evaluator_->Run(query_ptr,
                  reader,
                  writer,
                  /*const TJoinSubqueryProfiler &joinProfiler*/ nullptr,
                  /*const TConstFunctionProfilerMapPtr &functionProfilers*/ nullptr,
                  /*const TConstAggregateProfilerMapPtr &aggregateProfilers*/ nullptr,
                  /*const IMemoryChunkProviderPtr &memoryChunkProvider*/ GetDefaultMemoryChunkProvider(),
                  /*const TQueryBaseOptions &options*/ TQueryBaseOptions{},
                  /*const TFeatureFlags &requestFeatureFlags*/ MostFreshFeatureFlags(),
                  /*TFuture<TFeatureFlags> responseFeatureFlags*/ MakeFuture(MostFreshFeatureFlags()));
  // NYT::NQueryClient::TCGVariables fragmentParams;
  // auto queryInstance =
  //     Codegen(query, fragmentParams, joinProfiler, functionProfilers,
  //             aggregateProfilers, statistics, options.EnableCodeCache,
  //             options.UseCanonicalNullRelations, options.ExecutionBackend);
  //  ISchemafulUnversionedReaderPtr pipe_reader_ptr = pipe->GetReader();
  //  IUnversionedRowBatchPtr res = pipe_reader_ptr->Read();
  //  std::cout << "res row count " << res->GetRowCount() << std::endl;;

  //  IUnversionedRowsetWriterPtr writer;
  //  TFuture<NYT::NApi::IUnversionedRowsetPtr> asyncResultRowset;

   //    auto result = YsonToRows({
   //     "a=4;b=5",
   //     "a=10;b=11"
   // }, split);

   // Evaluate("a, b FROM [//t]", split, source, ResultMatcher(result));

   // EvaluateOnlyViaNativeExecutionBackend("a, b FROM [//t]", split, source,
   // ResultMatcher(result));

  // NYT::NCodegen::TCGModule::Create()

   auto resultRowset =
       NYT::NConcurrency::WaitFor(asyncResultRowset).ValueOrThrow();
   auto rows = resultRowset->GetRows();
   std::cout << "result rows " << rows.size();


  /*
   TCGVariables fragmentParams;
   TCGQueryInstance queryInstance = Codegen(query_ptr);
   */
  //  queryInstance.Run(TRange<TPIValue> literalValues, TRange<void *> opaqueData, TRange<size_t> opaqueDataSizes, TExecutionContext *context)
   
}

