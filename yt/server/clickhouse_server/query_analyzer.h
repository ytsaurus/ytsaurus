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
    std::vector<std::vector<TTablePtr>> Tables;
    std::vector<std::optional<DB::KeyCondition>> KeyConditions;
    std::optional<int> KeyColumnCount;
    EPoolKind PoolKind;
};

class TQueryAnalyzer
{
public:
    TQueryAnalyzer(const DB::Context& context, const DB::SelectQueryInfo& queryInfo);

    DB::ASTPtr RewriteQuery(
        const TRange<TSubquery> subqueries,
        TSubquerySpec specTemplate,
        const THashMap<NChunkClient::TChunkId, NChunkClient::TRefCountedMiscExtPtr>& miscExtMap,
        int subqueryIndex,
        bool isLastSubquery);

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
    bool Join_ = false;
    //! If the query contains global join.
    bool GlobalJoin_ = false;
    //! If the query contains outer right or outer full join.
    bool RightOrFullJoin_ = false;
    //! If the query joins two YT tables.
    bool TwoYTTableJoin_ = false;

    std::optional<NTableClient::TUnversionedOwningRow> PreviousUpperLimit_;

    std::vector<std::pair<DB::ASTPtr*, DB::ASTPtr>> Modifications_;

    void ParseQuery();

    void ValidateKeyColumns();

    std::shared_ptr<IStorageDistributor> GetStorage(const DB::ASTTableExpression* tableExpression) const;

    //! Apply modification to query part which can be later rolled back by calling RollbackModifications().
    void ApplyModification(DB::ASTPtr* queryPart, DB::ASTPtr newValue);
    //! Version with explicit previous value specifiction specially for weird DB::ASTSelect::refWhere() behaviour.
    void ApplyModification(DB::ASTPtr* queryPart, DB::ASTPtr newValue, DB::ASTPtr previousValue);
    //! Rollback all modifications to the query.
    void RollbackModifications();

    void ReplaceTableExpressions(std::vector<DB::ASTPtr> newTableExpressions);
    void AppendWhereCondition(
        std::optional<NTableClient::TUnversionedOwningRow> lowerLimit,
        std::optional<NTableClient::TUnversionedOwningRow> upperLimit);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
