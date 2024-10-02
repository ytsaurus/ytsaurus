#pragma once

#include "get_query_executor.h"
#include "select_query_executor.h"
#include "select_object_history_executor.h"
#include "transaction.h"
#include "continuation.h"
#include "watch_query_executor.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <class TProtoObjectOrderByExpression>
void FromProto(
    TObjectOrderByExpression* expression,
    const TProtoObjectOrderByExpression& protoExpression);

template <class TProtoObjectOrderByExpression>
void ToProto(
    TProtoObjectOrderByExpression* protoExpression,
    const TObjectOrderByExpression& expression);

template <class TProtoObjectOrderBy>
void FromProto(
    TObjectOrderBy* orderBy,
    const TProtoObjectOrderBy& protoOrderBy);

////////////////////////////////////////////////////////////////////////////////

template <class TProtoSelectContinuationTokenEvaluatedEntry>
void FromProto(
    TSelectContinuationTokenEvaluatedEntry* evaluatedEntry,
    const TProtoSelectContinuationTokenEvaluatedEntry& protoEvaluatedEntry);

template <class TProtoSelectContinuationTokenEvaluatedEntry>
void ToProto(
    TProtoSelectContinuationTokenEvaluatedEntry* protoEvaluatedEntry,
    const TSelectContinuationTokenEvaluatedEntry& evaluatedEntry);

////////////////////////////////////////////////////////////////////////////////

template <class TProtoSelectObjectsContinuationToken>
void FromProto(
    TSelectObjectsContinuationToken* continuationToken,
    const TProtoSelectObjectsContinuationToken& protoContinuationToken);

template <class TProtoSelectObjectsContinuationToken>
void ToProto(
    TProtoSelectObjectsContinuationToken* protoContinuationToken,
    const TSelectObjectsContinuationToken& continuationToken);

////////////////////////////////////////////////////////////////////////////////

template <class TProtoSelectObjectHistoryContinuationToken>
void FromProto(
    TSelectObjectHistoryContinuationToken* continuationToken,
    const TProtoSelectObjectHistoryContinuationToken& protoContinuationToken);

template <class TProtoSelectObjectHistoryContinuationToken>
void ToProto(
    TProtoSelectObjectHistoryContinuationToken* protoContinuationToken,
    const TSelectObjectHistoryContinuationToken& continuationToken);

////////////////////////////////////////////////////////////////////////////////

template <class TProtoRemoveUpdate>
void FromProto(
    TRemoveUpdateRequest* request,
    const TProtoRemoveUpdate& protoRequest);

template <class TProtoAttributeTimestampPrerequisite>
void FromProto(
    TAttributeTimestampPrerequisite* prerequisite,
    const TProtoAttributeTimestampPrerequisite& protoPrerequisite);

template <class TProtoGetObjectOptions>
void FromProto(
    TGetQueryOptions* options,
    const TProtoGetObjectOptions& protoOptions);

template <class TProtoSelectObjectsOptions, class TProtoSelectObjectsContinuationToken>
void FromProto(
    TSelectQueryOptions* options,
    const TProtoSelectObjectsOptions& protoOptions);

template <class TProtoTimeInterval>
void FromProto(
    TTimeInterval* timeInterval,
    const TProtoTimeInterval& protoTimeInterval);

template <class TProtoSelectObjectHistoryOptions>
void FromProto(
    TSelectObjectHistoryOptions* options,
    const TProtoSelectObjectHistoryOptions& protoOptions);

////////////////////////////////////////////////////////////////////////////////

template <class TProtoPerformanceStatistics>
void ToProto(
    TProtoPerformanceStatistics* protoStatistics,
    const TPerformanceStatistics& statistics);

////////////////////////////////////////////////////////////////////////////////

template <class TProtoWatchObjectsRequestPtr>
void FromProto(
    TWatchQueryOptions* queryOptions,
    const TProtoWatchObjectsRequestPtr& request);

template <class TProtoWatchQueryEventIndex>
void ToProto(
    TProtoWatchQueryEventIndex* protoEventIndex,
    const TWatchQueryEventIndex& eventIndex);

template <class TProtoWatchQueryEventIndex>
void FromProto(
    TWatchQueryEventIndex* eventIndex,
    const TProtoWatchQueryEventIndex& protoEventIndex);

////////////////////////////////////////////////////////////////////////////////

template <class TProtoTransactionOptions>
void FromProto(TTransactionOptions* options, const TProtoTransactionOptions& protoOptions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define CAST_INL_H_
#include "cast-inl.h"
#undef CAST_INL_H_
