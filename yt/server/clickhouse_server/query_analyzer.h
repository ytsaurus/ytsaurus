#pragma once

#include "private.h"

#include "storage_distributor.h"

#include <Storages/MergeTree/KeyCondition.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPoolKind,
    (Unordered)
    (Sorted)
)

struct TQueryAnalysisResult
{
    std::vector<std::vector<NYPath::TRichYPath>> TablePaths;
    std::vector<std::optional<DB::KeyCondition>> KeyConditions;
    EPoolKind PoolKind;
};

class TQueryAnalyzer
{
public:
    TQueryAnalyzer(const DB::Context& context, const DB::SelectQueryInfo& queryInfo);

    DB::ASTPtr RewriteQuery(
        const TRange<NChunkPools::TChunkStripeListPtr> stripeLists,
        TSubquerySpec specTemplate,
        int subqueryIndex);

    TQueryAnalysisResult Analyze();

private:
    const DB::Context& Context_;
    DB::SelectQueryInfo QueryInfo_;
    NLogging::TLogger Logger_;
    std::vector<DB::ASTTableExpression*> TableExpressions_;
    std::vector<DB::ASTPtr*> TableExpressionPtrs_;
    std::vector<std::shared_ptr<IStorageDistributor>> Storages_;

    std::shared_ptr<IStorageDistributor> GetStorage(const DB::ASTTableExpression* tableExpression) const;

    DB::ASTPtr ReplaceTableExpressions(std::vector<DB::ASTPtr> newTableExpressions);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
