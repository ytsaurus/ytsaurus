#pragma once

#include "cluster_nodes.h"
#include "private.h"
#include "query_analyzer.h"
#include "index_stats.h"

#include <QueryPipeline/Pipe.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct TDistributedQueryInfo
{
    DB::QueryProcessingStage::Enum ProcessingStage;
    DB::Header OutputHeader;
    std::vector<TSecondaryQuery> SecondaryQueries;
    TClusterNodes CliqueNodes;
    TSecondaryQueryReadTaskIteratorPtr TaskIterator;
};

class TDistributedQueryExecutor
{
public:
    TDistributedQueryExecutor(
        DB::ContextPtr context,
        TQueryContext* queryContext,
        TDistributedQueryInfo distributeInfo,
        DB::SelectQueryInfo queryInfo,
        NLogging::TLogger logger,
        i64 threadSubqueryCount,
        std::optional<TQueryAnalysisResult> queryAnalysisResult,
        std::vector<std::shared_ptr<IChytIndexStat>> indexStats);

    void ModifySecondaryQueries(std::function<void(DB::ASTPtr& secondaryQueryAst)> callback);

    void Fire();

    DB::Pipes ExtractPipes();
    DB::Pipe ExtractUnitedPipe();
    DB::QueryPipelineBuilderPtr ExtractPipeline(std::function<void()> commitCallback);

    std::vector<std::shared_ptr<IChytIndexStat>> ExtractIndexStats();

    DB::Header GetOutputHeader() const;
    bool PushDownPredicate() const;

private:
    const DB::ContextPtr Context_;
    TQueryContext* const QueryContext_;
    DB::SelectQueryInfo QueryInfo_;
    const NLogging::TLogger Logger;

    i64 ThreadSubqueryCount_;
    std::optional<TQueryAnalysisResult> QueryAnalysisResult_;

    TDistributedQueryInfo DistributeInfo_;

    std::vector<std::shared_ptr<IChytIndexStat>> IndexStats_;

    DB::Pipes Pipes_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
