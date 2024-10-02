#pragma once

#include "public.h"

#include <yt/yt/orm/client/objects/object_filter.h>
#include <yt/yt/orm/client/objects/transaction_context.h>

#include <yt/yt/orm/library/query/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/tablet_client/public.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TWatchQueryEventIndex
{
    int Tablet = 0;
    i64 Row = 0;
    i64 Event = 0;
    TTimestamp Timestamp = NTransactionClient::NullTimestamp;

    bool operator==(const TWatchQueryEventIndex& rhs) const = default;
};

struct TWatchQueryEvent
{
    TEventTypeName TypeName;
    TObjectId ObjectId;
    NYson::TYsonString Meta;
    TTimestamp Timestamp;
    int TabletReaderIndex;
    TWatchQueryEventIndex EventIndex;
    // True iff the event is accepted by the query filters.
    bool Accepted;
    TTransactionContext TransactionContext;
    TString ChangedAttributesSummary;
    THistoryTime HistoryTime;
    std::vector<i32> ChangedTags;
};

struct TStartFromEarliestOffsetTag
{ };

//! Start reading from last committed offset from queue consumer.
//! Works only when `Consumer` is non empty.
struct TStartFromLastCommittedOffsetTag
{ };

struct TWatchObjectsContinuationToken
{
    std::vector<TWatchQueryEventIndex> EventOffsets;
    int TabletCount = 0;
    TString WatchLog;
    std::optional<i64> DbVersion;
    TString SerializedToken;
    bool ReadAllTablets = false;
};

using TInitialOffsets = std::variant<
    TStartFromEarliestOffsetTag,
    TStartFromLastCommittedOffsetTag,
    TTimestamp,
    TWatchObjectsContinuationToken>;

struct TWatchQueryOptions
{
    TObjectTypeValue ObjectType;
    TString WatchLog;
    std::vector<int> Tablets;

    TInitialOffsets InitialOffsets;
    std::optional<TString> Consumer;

    std::optional<TTimestamp> Timestamp;
    std::optional<TDuration> TimeLimit;

    std::optional<i64> EventCountLimit;
    std::optional<TDuration> ReadTimeLimit;

    bool SkipTrimmed = false;
    bool FetchChangedAttributes = false;

    TObjectFilter Filter;
    THashSet<TString> Selector;
    TTagSet RequiredTags;
    TTagSet ExcludedTags;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TWatchQueryOptions& options,
    TStringBuf format);

void FormatValue(
    TStringBuilderBase* builder,
    const TStartFromEarliestOffsetTag& tag,
    TStringBuf format);

void FormatValue(
    TStringBuilderBase* builder,
    const TStartFromLastCommittedOffsetTag& tag,
    TStringBuf format);

void FormatValue(
    TStringBuilderBase* builder,
    const TWatchObjectsContinuationToken& token,
    TStringBuf format);

void FormatValue(
    TStringBuilderBase* builder,
    const TWatchQueryEventIndex& index,
    TStringBuf format);

struct TWatchQueryResult
{
    TTimestamp Timestamp = NullTimestamp;
    std::vector<TWatchQueryEvent> Events;
    TWatchObjectsContinuationToken ContinuationToken;
    ui64 SkippedRowCount = 0;
    std::vector<TString> ChangedAttributes;
};

////////////////////////////////////////////////////////////////////////////////

void ValidateInitialOffsetsConstruction(
    bool hasStartTimestamp,
    bool startFromEarliestOffset,
    bool hasContinuationToken);

TWatchObjectsContinuationToken DeserializeContinuationToken(const TString& serializedToken);

TString SerializeContinuationToken(const TWatchObjectsContinuationToken& token);

////////////////////////////////////////////////////////////////////////////////

struct IWatchQueryExecutor
{
    virtual TWatchQueryResult ExecuteWatchQuery(TWatchQueryOptions options) const = 0;

    virtual ~IWatchQueryExecutor() = default;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IWatchQueryExecutor> MakeWatchQueryExecutor(TTransactionPtr transaction);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
