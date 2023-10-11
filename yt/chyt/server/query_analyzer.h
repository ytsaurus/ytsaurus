#pragma once

#include "private.h"

#include "storage_distributor.h"

#include <yt/yt/client/table_client/key_bound.h>

#include <Storages/MergeTree/KeyCondition.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPoolKind,
    (Unordered)
    (Sorted)
);

struct TQueryAnalysisResult
{
    std::vector<NTableClient::TTableSchemaPtr> TableSchemas;
    std::vector<std::vector<TTablePtr>> Tables;
    std::vector<std::optional<DB::KeyCondition>> KeyConditions;
    std::optional<int> KeyColumnCount;
    EPoolKind PoolKind;
};

struct TSecondaryQuery
{
    DB::ASTPtr Query;
    DB::Scalars Scalars;
};

class TQueryAnalyzer
    : public DB::WithContext
{
public:
    TQueryAnalyzer(
        DB::ContextPtr context,
        const TStorageContext* storageContext,
        const DB::SelectQueryInfo& queryInfo,
        const NLogging::TLogger& logger);

    TSecondaryQuery CreateSecondaryQuery(
        const TRange<TSubquery>& subqueries,
        TSubquerySpec specTemplate,
        const THashMap<NChunkClient::TChunkId, NChunkClient::TRefCountedMiscExtPtr>& miscExtMap,
        int subqueryIndex,
        bool isLastSubquery);

    DB::QueryProcessingStage::Enum GetOptimizedQueryProcessingStage() const;

    TQueryAnalysisResult Analyze() const;

    bool HasJoinWithTwoTables() const;
    bool HasGlobalJoin() const;
    bool HasInOperator() const;

private:
    const TStorageContext* StorageContext_;
    DB::SelectQueryInfo QueryInfo_;
    NLogging::TLogger Logger;
    std::vector<DB::ASTTableExpression*> TableExpressions_;
    int YtTableCount_ = 0;
    std::vector<DB::ASTPtr*> TableExpressionPtrs_;
    std::vector<IStorageDistributorPtr> Storages_;
    //! If the query contains any kind of join.
    bool Join_ = false;
    //! If the query contains global join.
    bool GlobalJoin_ = false;
    //! If the query contains outer right or outer full join.
    bool RightOrFullJoin_ = false;
    //! If the query contains cross join.
    bool CrossJoin_ = false;
    //! If the query joins two YT tables.
    bool TwoYTTableJoin_ = false;
    //! If the query has in operator with subquery or table.
    bool HasInOperator_ = false;

    int KeyColumnCount_ = 0;
    bool JoinedByKeyColumns_ = false;

    std::optional<DB::QueryProcessingStage::Enum> OptimizedQueryProcessingStage_;

    std::vector<DB::ASTPtr> JoinKeyRightExpressions_;

    NTableClient::TOwningKeyBound PreviousUpperBound_;

    std::vector<std::pair<DB::ASTPtr*, DB::ASTPtr>> Modifications_;

    void ParseQuery();
    // Infer longest possible key prefix used in ON/USING clauses.
    // Throws an error if sorted pool is required, but key prefix is empty.
    void InferSortedJoinKeyColumns(bool needSortedPool);

    void OptimizeQueryProcessingStage();

    IStorageDistributorPtr GetStorage(const DB::ASTTableExpression* tableExpression) const;

    //! Apply modification to query part which can be later rolled back by calling RollbackModifications().
    void ApplyModification(DB::ASTPtr* queryPart, DB::ASTPtr newValue);
    //! Version with explicit previous value specification specially for weird DB::ASTSelect::refWhere() behaviour.
    void ApplyModification(DB::ASTPtr* queryPart, DB::ASTPtr newValue, DB::ASTPtr previousValue);
    //! Rollback all modifications to the query.
    void RollbackModifications();

    void ReplaceTableExpressions(std::vector<DB::ASTPtr> newTableExpressions);
    void AddBoundConditionToJoinedSubquery(
        NTableClient::TOwningKeyBound lowerBound,
        NTableClient::TOwningKeyBound upperBound);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
