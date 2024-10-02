#ifndef CAST_INL_H_
#error "Direct inclusion of this file is not allowed, include cast.h"
// For the sake of sane code completion.
#include "cast.h"
#endif

#include "public.h"

#include <yt/yt/orm/client/objects/transaction_context.h>
#include <yt/yt/orm/client/objects/tags.h>

#include <yt/yt/client/api/rpc_proxy/helpers.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/protobuf/interop/cast.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <class TProtoObjectOrderByExpression>
void FromProto(
    TObjectOrderByExpression* expression,
    const TProtoObjectOrderByExpression& protoExpression)
{
    expression->Expression = protoExpression.expression();
    expression->Descending = protoExpression.descending();
}

template <class TProtoObjectOrderByExpression>
void ToProto(
    TProtoObjectOrderByExpression* protoExpression,
    const TObjectOrderByExpression& expression)
{
    protoExpression->set_expression(expression.Expression);
    protoExpression->set_descending(expression.Descending);
}

template <class TProtoObjectOrderBy>
void FromProto(
    TObjectOrderBy* orderBy,
    const TProtoObjectOrderBy& protoOrderBy)
{
    for (const auto& protoExpression : protoOrderBy.expressions()) {
        FromProto(&orderBy->Expressions.emplace_back(), protoExpression);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TProtoSelectContinuationTokenEvaluatedEntry>
void FromProto(
    TSelectContinuationTokenEvaluatedEntry* evaluatedEntry,
    const TProtoSelectContinuationTokenEvaluatedEntry& protoEvaluatedEntry)
{
    evaluatedEntry->ObjectExpression = protoEvaluatedEntry.object_expression();
    evaluatedEntry->DBExpression = protoEvaluatedEntry.db_expression();
    evaluatedEntry->DBTableName = protoEvaluatedEntry.db_table_name();
    evaluatedEntry->DBEvaluated = protoEvaluatedEntry.db_evaluated();
    evaluatedEntry->Descending = protoEvaluatedEntry.descending();
    evaluatedEntry->YsonValue = NYson::TYsonString(protoEvaluatedEntry.yson_value());
}

template <class TProtoSelectContinuationTokenEvaluatedEntry>
void ToProto(
    TProtoSelectContinuationTokenEvaluatedEntry* protoEvaluatedEntry,
    const TSelectContinuationTokenEvaluatedEntry& evaluatedEntry)
{
    protoEvaluatedEntry->set_object_expression(evaluatedEntry.ObjectExpression);
    protoEvaluatedEntry->set_db_expression(evaluatedEntry.DBExpression);
    protoEvaluatedEntry->set_db_table_name(evaluatedEntry.DBTableName);
    protoEvaluatedEntry->set_db_evaluated(evaluatedEntry.DBEvaluated);
    protoEvaluatedEntry->set_descending(evaluatedEntry.Descending);
    protoEvaluatedEntry->set_yson_value(evaluatedEntry.YsonValue.ToString());
}

////////////////////////////////////////////////////////////////////////////////

template <class TProtoSelectObjectsContinuationToken>
void FromProto(
    TSelectObjectsContinuationToken* continuationToken,
    const TProtoSelectObjectsContinuationToken& protoContinuationToken)
{
    continuationToken->MajorVersion = protoContinuationToken.major_version();
    continuationToken->MinorVersion = protoContinuationToken.minor_version();
    continuationToken->EvaluatedEntries.clear();
    for (const auto& protoEvaluatedEntry : protoContinuationToken.evaluated_entries()) {
        FromProto(&continuationToken->EvaluatedEntries.emplace_back(), protoEvaluatedEntry);
    }
}

template <class TProtoSelectObjectsContinuationToken>
void ToProto(
    TProtoSelectObjectsContinuationToken* protoContinuationToken,
    const TSelectObjectsContinuationToken& continuationToken)
{
    protoContinuationToken->set_major_version(continuationToken.MajorVersion);
    protoContinuationToken->set_minor_version(continuationToken.MinorVersion);
    protoContinuationToken->clear_evaluated_entries();
    for (const auto& evaluatedEntry : continuationToken.EvaluatedEntries) {
        ToProto(protoContinuationToken->add_evaluated_entries(), evaluatedEntry);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TProtoSelectObjectHistoryContinuationToken>
void FromProto(
    TSelectObjectHistoryContinuationToken* continuationToken,
    const TProtoSelectObjectHistoryContinuationToken& protoContinuationToken)
{
    continuationToken->MajorVersion = protoContinuationToken.major_version();
    continuationToken->MinorVersion = protoContinuationToken.minor_version();
    continuationToken->ReadSource = protoContinuationToken.read_source();
    continuationToken->ReadPhase = CheckedEnumCast<EHistoryReadPhase>(protoContinuationToken.read_phase());
    continuationToken->EvaluatedEntries.clear();
    for (const auto& protoEvaluatedEntry : protoContinuationToken.evaluated_entries()) {
        FromProto(&continuationToken->EvaluatedEntries.emplace_back(), protoEvaluatedEntry);
    }
}

template <class TProtoSelectObjectHistoryContinuationToken>
void ToProto(
    TProtoSelectObjectHistoryContinuationToken* protoContinuationToken,
    const TSelectObjectHistoryContinuationToken& continuationToken)
{
    protoContinuationToken->set_major_version(continuationToken.MajorVersion);
    protoContinuationToken->set_minor_version(continuationToken.MinorVersion);
    protoContinuationToken->set_read_source(continuationToken.ReadSource);
    protoContinuationToken->set_read_phase(static_cast<int>(continuationToken.ReadPhase));
    protoContinuationToken->clear_evaluated_entries();
    for (const auto& evaluatedEntry : continuationToken.EvaluatedEntries) {
        ToProto(protoContinuationToken->add_evaluated_entries(), evaluatedEntry);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TProtoRemoveUpdate>
void FromProto(
    TRemoveUpdateRequest* request,
    const TProtoRemoveUpdate& protoRequest)
{
    request->Path = protoRequest.path();
    request->Force = protoRequest.force();
}

template <class TProtoAttributeTimestampPrerequisite>
void FromProto(
    TAttributeTimestampPrerequisite* prerequisite,
    const TProtoAttributeTimestampPrerequisite& protoPrerequisite)
{
    prerequisite->Path = protoPrerequisite.path();
    prerequisite->Timestamp = protoPrerequisite.timestamp();
}

template <class TProtoGetObjectOptions>
void FromProto(
    TGetQueryOptions* options,
    const TProtoGetObjectOptions& protoOptions)
{
    if (protoOptions.ignore_nonexistent() && protoOptions.skip_nonexistent()) {
        THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
            "Options `ignore_nonexistent` and `skip_nonexistent` must not be set at the same time");
    }

    if (protoOptions.ignore_nonexistent()) {
        options->NonexistentObjectPolicy = ENonexistentObjectPolicy::Ignore;
    }

    if (protoOptions.skip_nonexistent()) {
        options->NonexistentObjectPolicy = ENonexistentObjectPolicy::Skip;
    }

    options->FetchValues = protoOptions.fetch_values();
    options->FetchTimestamps = protoOptions.fetch_timestamps();
    options->FetchRootObject = protoOptions.fetch_root_object();
}

template <class TProtoSelectObjectsOptions, class TProtoSelectObjectsContinuationToken>
void FromProto(
    TSelectQueryOptions* options,
    const TProtoSelectObjectsOptions& protoOptions)
{
    options->FetchValues = protoOptions.fetch_values();
    options->FetchTimestamps = protoOptions.fetch_timestamps();
    if (protoOptions.has_offset()) {
        options->Offset.emplace(protoOptions.offset());
    }
    if (protoOptions.has_limit()) {
        options->Limit.emplace(protoOptions.limit());
    }
    if (protoOptions.has_continuation_token()) {
        TProtoSelectObjectsContinuationToken protoContinuationToken;
        DeserializeContinuationToken(protoOptions.continuation_token(), &protoContinuationToken);
        FromProto(&options->ContinuationToken.emplace(), protoContinuationToken);
        options->ContinuationToken->SerializedToken = protoOptions.continuation_token();
    }
    options->FetchRootObject = protoOptions.fetch_root_object();
    if (protoOptions.has_fetch_finalizing_objects()) {
        options->FetchFinalizingObjects.emplace(protoOptions.fetch_finalizing_objects());
    }
    options->EnablePartialResult = protoOptions.enable_partial_result();
}

template <class TProtoTimeInterval>
void FromProto(
    TTimeInterval* timeInterval,
    const TProtoTimeInterval& protoTimeInterval)
{
    if constexpr (std::same_as<google::protobuf::Timestamp, std::decay_t<decltype(protoTimeInterval.begin())>>) {
        if (protoTimeInterval.has_begin()) {
            timeInterval->Begin.emplace<TInstant>(NProtoInterop::CastFromProto(protoTimeInterval.begin()));
        }
        if (protoTimeInterval.has_end()) {
            timeInterval->End.emplace<TInstant>(NProtoInterop::CastFromProto(protoTimeInterval.end()));
        }
    } else {
        if (protoTimeInterval.has_begin()) {
            timeInterval->Begin.emplace<TTimestamp>(protoTimeInterval.begin());
        }
        if (protoTimeInterval.has_end()) {
            timeInterval->End.emplace<TTimestamp>(protoTimeInterval.end());
        }
    }
}

template <class TProtoSelectObjectHistoryOptions>
void FromProto(
    TSelectObjectHistoryOptions* options,
    const TProtoSelectObjectHistoryOptions& protoOptions)
{
    options->Uuid = protoOptions.uuid();
    options->Limit = protoOptions.limit();

    if (protoOptions.has_interval()) {
        FromProto(&options->TimeInterval, protoOptions.interval());
    }
    if (protoOptions.has_timestamp_interval()) {
        FromProto(&options->TimeInterval, protoOptions.timestamp_interval());
    }

    options->DescendingTimeOrder = protoOptions.descending_time_order();

    if (protoOptions.has_index_mode()) {
        NYT::FromProto(&options->IndexMode, protoOptions.index_mode());
    }

    options->AllowTimeModeConversion = protoOptions.allow_time_mode_conversion();
    options->FetchRootObject = protoOptions.fetch_root_object();
    options->Filter = protoOptions.filter();
}

////////////////////////////////////////////////////////////////////////////////

template <class TProtoPerformanceStatistics>
void ToProto(
    TProtoPerformanceStatistics* protoStatistics,
    const TPerformanceStatistics& statistics)
{
    protoStatistics->set_read_phase_count(statistics.ReadPhaseCount);
    for (const auto& [tableName, selectStatistics] : statistics.SelectQueryStatistics) {
        auto* protoSelectStatistics = protoStatistics->add_select_query_statistics();
        protoSelectStatistics->set_table_name(tableName);
        for (const auto& statistic : selectStatistics) {
            auto* protoStatistic = protoSelectStatistics->add_statistics();
            NYT::NApi::NRpcProxy::NProto::ToProto(protoStatistic, statistic);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TProtoWatchObjectsRequestPtr>
void FromProto(
    TWatchQueryOptions* queryOptions,
    const TProtoWatchObjectsRequestPtr& request)
{
    queryOptions->WatchLog = request->watch_log();
    if (!request->tablets().empty()) {
        NYT::FromProto(&(queryOptions->Tablets), request->tablets());
    }

    ValidateInitialOffsetsConstruction(
        request->has_start_timestamp(),
        request->start_from_earliest_offset(),
        request->has_continuation_token());

    if (request->has_start_timestamp()) {
        queryOptions->InitialOffsets = static_cast<TTimestamp>(request->start_timestamp());
    }

    if (request->start_from_earliest_offset()) {
        queryOptions->InitialOffsets = TStartFromEarliestOffsetTag{};
    }

    if (request->has_continuation_token()) {
        queryOptions->InitialOffsets = DeserializeContinuationToken(request->continuation_token());
    }

    if (request->has_timestamp()) {
        queryOptions->Timestamp = request->timestamp();
    }
    if (request->has_time_limit()) {
        const auto& protoTimeLimit = request->time_limit();
        queryOptions->TimeLimit = NProtoInterop::CastFromProto(protoTimeLimit);
    }

    if (request->has_event_count_limit()) {
        queryOptions->EventCountLimit = request->event_count_limit();
    }
    if (request->has_read_time_limit()) {
        const auto& protoTimeLimit = request->read_time_limit();
        queryOptions->ReadTimeLimit = NProtoInterop::CastFromProto(protoTimeLimit);
    }

    queryOptions->SkipTrimmed = request->skip_trimmed();
    queryOptions->FetchChangedAttributes = request->fetch_changed_attributes();

    queryOptions->Filter.Query = request->filter().query();
    NYT::FromProto(&queryOptions->Selector, request->selector().paths());
    if (!request->required_tags().empty()) {
        NClient::NObjects::FromProto(&(queryOptions->RequiredTags), request->required_tags());
    }
    if (!request->excluded_tags().empty()) {
        NClient::NObjects::FromProto(&(queryOptions->ExcludedTags), request->excluded_tags());
    }
}

template <class TProtoWatchQueryEventIndex>
void ToProto(
    TProtoWatchQueryEventIndex* protoEventIndex,
    const TWatchQueryEventIndex& eventIndex)
{
    protoEventIndex->set_tablet(eventIndex.Tablet);
    protoEventIndex->set_row(eventIndex.Row);
    protoEventIndex->set_event(eventIndex.Event);
}

template <class TProtoWatchQueryEventIndex>
void FromProto(
    TWatchQueryEventIndex* eventIndex,
    const TProtoWatchQueryEventIndex& protoEventIndex)
{
    eventIndex->Tablet = protoEventIndex.tablet();
    eventIndex->Row = protoEventIndex.row();
    eventIndex->Event = protoEventIndex.event();
}

////////////////////////////////////////////////////////////////////////////////

template <class TProtoTransactionOptions>
void FromProto(TMutatingTransactionOptions* options, const TProtoTransactionOptions& protoOptions)
{
    NClient::NObjects::FromProto(
        &options->TransactionContext,
        protoOptions.transaction_context());
    if (protoOptions.has_allow_removal_with_non_empty_reference()) {
        options->AllowRemovalWithNonemptyReferences.emplace(
            protoOptions.allow_removal_with_non_empty_reference());
    }
    options->SkipWatchLog = protoOptions.skip_watch_log();
    options->SkipHistory = protoOptions.skip_history();
    options->SkipRevisionBump = protoOptions.skip_revision_bump();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
