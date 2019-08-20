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
    std::vector<NTableClient::TTableSchema> TableSchemas;
    std::vector<std::vector<NYPath::TRichYPath>> TablePaths;
    std::vector<std::optional<DB::KeyCondition>> KeyConditions;
    std::optional<int> KeyColumnCount;
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
    NLogging::TLogger Logger;
    std::vector<DB::ASTTableExpression*> TableExpressions_;
    int YtTableCount_ = 0;
    std::vector<DB::ASTPtr*> TableExpressionPtrs_;
    std::vector<std::shared_ptr<IStorageDistributor>> Storages_;
    //! If the query contains any kind of join.
    bool IsJoin_ = false;
    //! If the query contains global join.
    bool IsGlobalJoin_ = false;
    //! If the query is about outer right or outer full join.
    bool IsRightOrFullJoin_ = false;
    //! If the query joins two YT tables.
    bool IsTwoYtTableJoin_ = false;

    void ParseQuery();

    void ValidateKeyColumns();

    void AppendWhereCondition();

    std::shared_ptr<IStorageDistributor> GetStorage(const DB::ASTTableExpression* tableExpression) const;

    DB::ASTPtr ReplaceTableExpressions(std::vector<DB::ASTPtr> newTableExpressions);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
