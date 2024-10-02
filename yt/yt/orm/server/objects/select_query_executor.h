#pragma once

#include "public.h"
#include "attribute_values_consumer.h"
#include "transaction.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TSelectQueryOptions
{
    bool FetchValues = true;
    bool FetchTimestamps = false;
    std::optional<i64> Offset;
    std::optional<i64> Limit;
    std::optional<i64> MasterSideLimit;
    std::optional<TSelectObjectsContinuationToken> ContinuationToken;
    bool FetchRootObject = false;
    bool CheckReadPermissions = false;
    std::optional<bool> FetchFinalizingObjects;
    bool EnablePartialResult = false;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TSelectQueryOptions& options,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TSelectQueryResult
{
    std::optional<TSelectObjectsContinuationToken> ContinuationToken;
    bool LimitSatisfied = false;
};

struct TSelectQueryResultWithPayload
    : TSelectQueryResult
{
    std::vector<TAttributeValueList> Objects;
};

////////////////////////////////////////////////////////////////////////////////

struct ISelectQueryExecutor
{
    virtual TSelectQueryResultWithPayload Execute() && = 0;

    virtual TSelectQueryResult ExecuteConsuming(
        IAttributeValuesConsumerGroup* consumerGroup) const = 0;

    virtual void PatchOptions(std::optional<TSelectObjectsContinuationToken> token, i64 masterSideLimit) = 0;
    virtual bool CheckRequestRemainingTimeoutExpired() const = 0;

    virtual ~ISelectQueryExecutor() = default;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ISelectQueryExecutor> MakeSelectQueryExecutor(
    TTransactionPtr transaction,
    TObjectTypeValue type,
    const std::optional<TObjectFilter>& filter,
    const TAttributeSelector& selector,
    const TObjectOrderBy& orderBy,
    const TSelectQueryOptions& options,
    const std::optional<TIndex>& index = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
