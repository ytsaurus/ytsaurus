#pragma once

#include "public.h"
#include "transaction.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TSelectObjectHistoryOptions
{
    TString Uuid;
    int Limit = 0;
    TTimeInterval TimeInterval;
    bool DescendingTimeOrder = false;
    bool FetchRootObject = false;
    ESelectObjectHistoryIndexMode IndexMode = ESelectObjectHistoryIndexMode::Default;
    bool AllowTimeModeConversion = false;
    TString Filter;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TSelectObjectHistoryOptions& options,
    TStringBuf format);

struct TSelectObjectHistoryResult
{
    std::vector<THistoryEvent> Events;
    TSelectObjectHistoryContinuationToken ContinuationToken;
    bool IndexUsed = false;
    TInstant LastTrimTime;
};

////////////////////////////////////////////////////////////////////////////////

struct ISelectObjectHistoryExecutor
{
    virtual TSelectObjectHistoryResult Execute() && = 0;
    virtual ~ISelectObjectHistoryExecutor() = default;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ISelectObjectHistoryExecutor> MakeSelectObjectHistoryExecutor(
    TTransactionPtr transaction,
    TObjectTypeValue objectType,
    const TObjectKey& objectKey,
    const TAttributeSelector& attributeSelector,
    const std::optional<TAttributeSelector>& distinctBySelector,
    std::optional<TSelectObjectHistoryContinuationToken> continuationToken,
    TSelectObjectHistoryOptions options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
