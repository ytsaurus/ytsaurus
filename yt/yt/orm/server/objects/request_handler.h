#pragma once

#include "attribute_matcher.h"
#include "get_query_executor.h"
#include "select_object_history_executor.h"
#include "select_query_executor.h"
#include "transaction.h"
#include "transaction_manager.h"
#include "watch_query_executor.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TCommonOptions
{
    std::optional<bool> AllowFullScan;
    bool CollectStatistics = false;
};

template<typename TRequestCommonOptions>
TCommonOptions GetCommonOptions(const TRequestCommonOptions& commonOptions);

void FormatValue(
    TStringBuilderBase* builder,
    const TCommonOptions& options,
    TStringBuf format);

struct TCommonWriteResult
{
    TTransactionId TransactionId;
    std::optional<TTimestamp> CommitTimestamp;
    std::optional<TPerformanceStatistics> PerformanceStatistics;
};

struct TCommonReadResult
{
    TTimestamp StartTimestamp = NullTimestamp;
    std::optional<TPerformanceStatistics> PerformanceStatistics;
};

////////////////////////////////////////////////////////////////////////////////

struct TCreateObjectsRequest
{
    TCommonOptions CommonOptions;
    std::vector<TCreateObjectSubrequest> Subrequests;
    bool NeedIdentityMeta = false;
};

struct TObjectInfo
{
    TObjectKey Key;
    TObjectTypeValue Type;
    TString Fqid;
    NYson::TYsonString IdentityMeta;
};

struct TCreateObjectsResponse
{
    TCommonWriteResult CommonWriteResult;
    std::vector<TObjectInfo> ObjectInfos;
};

////////////////////////////////////////////////////////////////////////////////

struct TRemoveObjectsRequest
{
    struct TSubrequest
    {
        TObjectTypeValue Type;
        TKeyAttributeMatches Match;
    };

    TCommonOptions CommonOptions;
    std::vector<TSubrequest> Subrequests;
    bool IgnoreNonexistent = false;
    // NB: This option is applied for the whole transaction.
    std::optional<bool> AllowRemovalWithNonEmptyReferences;
    bool NeedIdentityMeta = false;
};

struct TRemoveObjectsResponse
{
    struct TSubresponse
    {
        std::optional<TObjectInfo> ObjectInfo;
        TInstant FinalizationStartTime;
    };

    TCommonWriteResult CommonWriteResult;
    std::vector<TSubresponse> Subresponses;
};

////////////////////////////////////////////////////////////////////////////////

struct TUpdateObjectsRequest
{
    struct TSubrequest
    {
        TObjectTypeValue Type;
        TKeyAttributeMatches Match;
        std::vector<TUpdateRequest> Updates;
        std::vector<TAttributeTimestampPrerequisite> Prerequisites;
        IAttributeValuesConsumer* Consumer;
    };

    TCommonOptions CommonOptions;
    std::vector<TSubrequest> Subrequests;
    bool IgnoreNonexistent = false;
    bool NeedIdentityMeta = false;
};

struct TUpdateObjectsResponse
{
    TCommonWriteResult CommonWriteResult;
    std::vector<std::optional<TObjectInfo>> ObjectInfos;
};

////////////////////////////////////////////////////////////////////////////////

struct TGetObjectsRequest
{
    TCommonOptions CommonOptions;
    TAttributeSelector Selector;
    TGetQueryOptions QueryOptions;
    TObjectTypeValue ObjectType;
    TTimestampOrTransactionId TimestampOrTransactionId;
};

struct TGetObjectsResponse
{
    TCommonReadResult CommonReadResult;
};

////////////////////////////////////////////////////////////////////////////////

struct TWatchObjectsRequest
{
    TCommonOptions CommonOptions;
    TWatchQueryOptions QueryOptions;
};

struct TWatchObjectsResponse
{
    TWatchQueryResult QueryResult;
    std::optional<TPerformanceStatistics> PerformanceStatistics;
};

////////////////////////////////////////////////////////////////////////////////

struct TSelectObjectsRequest
{
    TCommonOptions CommonOptions;
    TAttributeSelector Selector;
    TSelectQueryOptions QueryOptions;
    std::optional<TObjectFilter> Filter;
    TObjectOrderBy OrderBy;
    std::optional<TIndex> Index;
    TObjectTypeValue ObjectType;
    TTimestampOrTransactionId TimestampOrTransactionId;
};

struct TSelectObjectsResponse
{
    TCommonReadResult CommonReadResult;
    TSelectQueryResult QueryResult;
};

////////////////////////////////////////////////////////////////////////////////

struct TAggregateObjectsRequest
{
    TCommonOptions CommonOptions;
    std::optional<TObjectFilter> Filter;
    TAttributeAggregateExpressions AggregateExpressions;
    TAttributeGroupingExpressions GroupByExpressions;
    TObjectTypeValue ObjectType;
    TTimestampOrTransactionId TimestampOrTransactionId;
    std::optional<bool> FetchFinalizingObjects;
};

struct TAggregateObjectsResponse
{
    TCommonReadResult CommonReadResult;
    TAggregateQueryResult QueryResult;
    TAttributeSelector Selector;
};

////////////////////////////////////////////////////////////////////////////////

struct TSelectObjectHistoryRequest
{
    TCommonOptions CommonOptions;
    TKeyAttributeMatches Match;
    TSelectObjectHistoryOptions SelectOptions;
    TObjectTypeValue ObjectType;
    std::optional<TSelectObjectHistoryContinuationToken> ContinuationToken;
};

struct TSelectObjectHistoryResponse
{
    TCommonReadResult CommonReadResult;
    TSelectObjectHistoryResult QueryResult;
};

////////////////////////////////////////////////////////////////////////////////

struct TStartTransactionResponse
{
    TTransactionId TransactionId;
    TTimestamp StartTimestamp;
    TInstant StartTime;
};

struct TCommitTransactionOptions
{
    TTransactionContext TransactionContext;
    bool CollectStatistics = false;
};

struct TCommitTransactionResponse
{
    TTransactionCommitResult CommitResult;
    std::optional<TPerformanceStatistics> PerformanceStatistics;
    std::optional<TPerformanceStatistics> TotalPerformanceStatistics;
};

////////////////////////////////////////////////////////////////////////////////

struct TWatchLogInfoResponse
{
    int TabletCount;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest>
concept CCanBeDryRun = std::is_same_v<TRequest, TUpdateObjectsRequest> ||
    std::is_same_v<TRequest, TRemoveObjectsRequest> ||
    std::is_same_v<TRequest, TCreateObjectsRequest>;

////////////////////////////////////////////////////////////////////////////////

class TRequestHandler final
{
public:
    TRequestHandler(NMaster::IBootstrap* bootstrap, bool enforceReadPermissions);

    TTimestamp GenerateTimestamps(
        const NRpc::IServiceContextPtr& context,
        int count = 1) const;

    TStartTransactionResponse StartTransaction(
        const NRpc::IServiceContextPtr& context,
        TStartReadWriteTransactionOptions&& readWriteTransactionOptions) const;

    TCommitTransactionResponse CommitTransaction(
        const NRpc::IServiceContextPtr& context,
        TTransactionId transactionId,
        TCommitTransactionOptions&& options) const;

    void AbortTransaction(
        const NRpc::IServiceContextPtr& context,
        TTransactionId transactionId) const;

    TCreateObjectsResponse CreateObjects(
        const NRpc::IServiceContextPtr& context,
        TTransactionId transactionId,
        TCreateObjectsRequest&& request) const;

    TRemoveObjectsResponse RemoveObjects(
        const NRpc::IServiceContextPtr& context,
        TTransactionId transactionId,
        TRemoveObjectsRequest&& request) const;

    TUpdateObjectsResponse UpdateObjects(
        const NRpc::IServiceContextPtr& context,
        TTransactionId transactionId,
        TUpdateObjectsRequest&& request) const;

    TGetObjectsResponse GetObjects(
        const NRpc::IServiceContextPtr& context,
        const std::vector<TKeyAttributeMatches>& matches,
        IAttributeValuesConsumerGroup* consumerGroup,
        TGetObjectsRequest&& request) const;

    TWatchObjectsResponse WatchObjects(
        const NRpc::IServiceContextPtr& context,
        TWatchObjectsRequest&& request) const;

    TSelectObjectsResponse SelectObjects(
        const NRpc::IServiceContextPtr& context,
        IAttributeValuesConsumerGroup* consumerGroup,
        TSelectObjectsRequest&& request) const;

    TAggregateObjectsResponse AggregateObjects(
        const NRpc::IServiceContextPtr& context,
        TAggregateObjectsRequest&& request) const;

    TSelectObjectHistoryResponse SelectObjectHistory(
        const NRpc::IServiceContextPtr& context,
        const TAttributeSelector& selector,
        const std::optional<TAttributeSelector>& distinctBy,
        TSelectObjectHistoryRequest&& request) const;

    TWatchLogInfoResponse GetWatchLogInfo(
        const NRpc::IServiceContextPtr& context,
        TObjectTypeValue objectType,
        const TString& logName) const;

    void AdvanceWatchLogConsumer(
        const NRpc::IServiceContextPtr& context,
        TObjectTypeValue objectType,
        const TString& consumer,
        const TWatchObjectsContinuationToken& continuationToken) const;

    template <CCanBeDryRun TRequest>
    void RunPrecommitActions(
        const NRpc::IServiceContextPtr& context,
        TTransactionId transactionId,
        TRequest&& request) const;

private:
    NMaster::IBootstrap* Bootstrap_;
    const bool EnforceReadPermissions_;

    TRemoveObjectsResponse DoRemoveObjects(
        const NRpc::IServiceContextPtr& context,
        TTransactionId transactionId,
        TRemoveObjectsRequest&& request,
        bool dryRun = false) const;

    TUpdateObjectsResponse DoUpdateObjects(
        const NRpc::IServiceContextPtr& context,
        TTransactionId transactionId,
        TUpdateObjectsRequest&& request,
        bool dryRun = false) const;

    TCreateObjectsResponse DoCreateObjects(
        const NRpc::IServiceContextPtr& context,
        TTransactionId transactionId,
        TCreateObjectsRequest&& request,
        bool dryRun = false) const;

    bool IsSchemaReadAllowed(TObjectTypeValue type) const;
    bool IsSchemaLimitedReadAllowed(TObjectTypeValue type) const;
    void EnforceObjectHistoryReadPermission(
        const TTransactionPtr& transaction,
        const TAttributeSelector& attributeSelector,
        TSelectObjectHistoryRequest& request) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define REQUEST_HANDLER_INL_H_
#include "request_handler-inl.h"
#undef REQUEST_HANDLER_INL_H_
