#pragma once

#include "private.h"

#include <yt/yt/client/table_client/key_bound.h>

#include <Storages/MergeTree/KeyCondition.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPoolKind,
    (Unordered)
    (Sorted)
);

DEFINE_ENUM(EReadInOrderMode,
    (None)
    (Forward)
    (Backward)
);

struct TQueryAnalysisResult
{
    std::vector<NTableClient::TTableSchemaPtr> TableSchemas;
    std::vector<std::vector<TTablePtr>> Tables;
    std::vector<std::optional<DB::KeyCondition>> KeyConditions;
    std::optional<int> KeyColumnCount;
    EPoolKind PoolKind;
    EReadInOrderMode ReadInOrderMode = EReadInOrderMode::None;
    bool JoinedByKeyColumns;
};

struct TSecondaryQuery
{
    DB::ASTPtr Query;
    DB::Scalars Scalars;
    DB::UInt64 TotalRowsToRead;
    DB::UInt64 TotalBytesToRead;
};

struct TBoundJoinOptions
{
    bool FilterJoinedSubqueryBySortKey = false;
    bool RightOrFullJoin = false;
    bool CareAboutNullsInBoundCondition = false;
    NTableClient::TTableSchemaPtr JoinLeftTableExpressionSchema;
    DB::QueryTreeNodePtr JoinRightTableExpression;
    std::vector<DB::QueryTreeNodePtr> JoinRightKeyExpressions;
};

class TSecondaryQueryBuilder
{
public:
    TSecondaryQueryBuilder(
        DB::ContextPtr context,
        const NLogging::TLogger& logger,
        DB::QueryTreeNodePtr query,
        const std::vector<TSubquerySpec>& operandSpecs,
        TBoundJoinOptions boundJoinOptions = {});

    TSecondaryQuery CreateSecondaryQuery(int inputStreamsPerSecondaryQuery);

    TSecondaryQuery CreateSecondaryQuery(
        const TRange<TSubquery>& subqueries,
        const THashMap<NChunkClient::TChunkId, NChunkClient::TRefCountedMiscExtPtr>& miscExtMap,
        int subqueryIndex,
        bool isLastSubquery);

private:
    NLogging::TLogger Logger;
    const DB::ContextPtr Context_;
    const DB::QueryTreeNodePtr Query_;
    const std::vector<TSubquerySpec> OperandSpecs_;
    const TBoundJoinOptions BoundJoinOptions_;

    NTableClient::TOwningKeyBound PreviousUpperBound_;

    DB::QueryTreeNodePtr AddBoundConditionToJoinedSubquery(
        DB::QueryTreeNodePtr query,
        DB::QueryTreeNodePtr joinedTableExpression,
        NTableClient::TOwningKeyBound lowerBound,
        NTableClient::TOwningKeyBound upperBound);
};

class TQueryAnalyzer
    : public DB::WithContext
{
public:
    TQueryAnalyzer(
        DB::ContextPtr context,
        const TStorageContext* storageContext,
        const DB::SelectQueryInfo& queryInfo,
        const NLogging::TLogger& logger,
        bool onlyAnalyze = false);

    //! TQueryAnalyzer should be prepared before CreateSecondaryQuery,
    //! GetOptimizedQueryProcessingStage and Analyze methods are called.
    void Prepare();

    //! Prepare method should be called before GetSecondaryQueryBuilder.
    std::shared_ptr<TSecondaryQueryBuilder> GetSecondaryQueryBuilder(
        TSubquerySpec specTemplate);

    //! Prepare method should be called before GetOptimizedQueryProcessingStage.
    DB::QueryProcessingStage::Enum GetOptimizedQueryProcessingStage() const;

    //! Prepare method should be called before GetReadInOrderMode.
    EReadInOrderMode GetReadInOrderMode() const;

    //! Prepare method should be called before Analyze.
    TQueryAnalysisResult Analyze() const;

    bool HasJoinWithTwoTables() const;
    bool HasRightOrFullJoin() const;
    bool HasGlobalJoin() const;
    bool HasInOperator() const;
    bool NeedOnlyDistinct() const;

    bool IsJoinedByKeyColumns() const;

private:
    const TStorageContext* StorageContext_;
    DB::SelectQueryInfo QueryInfo_;
    NLogging::TLogger Logger;
    std::vector<DB::QueryTreeNodePtr> TableExpressions_;
    std::vector<DB::TableExpressionData*> TableExpressionDataPtrs_;
    int YtTableCount_ = 0;
    int SecondaryQueryOperandCount_ = 0;
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
    //! If the query needs only distinct values of one column for execution.
    bool NeedOnlyDistinct_ = false;

    bool Prepared_ = false;
    bool OnlyAnalyze_;

    int KeyColumnCount_ = 0;
    bool JoinedByKeyColumns_ = false;

    std::optional<DB::QueryProcessingStage::Enum> OptimizedQueryProcessingStage_;

    EReadInOrderMode ReadInOrderMode_ = EReadInOrderMode::None;

    std::vector<DB::QueryTreeNodePtr> JoinKeyRightExpressions_;

    NTableClient::TOwningKeyBound PreviousUpperBound_;

    void ParseQuery();
    // Infer longest possible key prefix used in ON/USING clauses.
    // Throws an error if sorted pool is required, but key prefix is empty.
    void InferSortedJoinKeyColumns(bool needSortedPool);

    void OptimizeQueryProcessingStage();

    void InferReadInOrderMode(bool assumeNoNullKeys, bool assumeNoNanKeys);

    IStorageDistributorPtr GetStorage(const DB::QueryTreeNodePtr& tableExpression) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
