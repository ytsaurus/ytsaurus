#pragma once

#include "public.h"

#include "payload.h"

#include <yt/yt/orm/client/objects/transaction_context.h>
#include <yt/yt/orm/client/objects/type.h>

#include <yt/yt_proto/yt/orm/client/proto/object.pb.h>

#include <yt/yt/client/table_client/schema.h>

#include <optional>

// Some clients of yt/cpp/roren use ORM in pipeline. To Pass ORM struct across pipeline,
// structs must be serializable/deserializable using ::Save/::Load from util/ysaveload.h.
// To declare Save/Load methods, add Y_SAVELOAD_DEFINE marco to structs.

namespace NYT::NOrm::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

template <typename TPayloadProto>
void ToProto(
    TPayloadProto* protoPayload,
    const TObjectIdentity& objectIdentity);

////////////////////////////////////////////////////////////////////////////////

struct TObjectOrderByExpression
{
    TString Expression;
    bool Descending = false;
};

using TObjectOrderBy = std::vector<TObjectOrderByExpression>;

void FormatValue(
    TStringBuilderBase* builder,
    const TObjectOrderByExpression& orderByExpression,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TAdaptiveBatchSizeOptions
{
    int IncreasingAdditive = 10;
    int DecreasingDivisor = 2;
    int MaxBatchSize = 500;
    int MinBatchSize = 1;
    std::optional<int> MaxConsecutiveRetryCount;

    void Validate() const;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TAdaptiveBatchSizeOptions& options,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TSelectObjectsOptions
{
    NObjects::TTimestamp Timestamp = NObjects::NullTimestamp;
    EPayloadFormat Format = EPayloadFormat::Yson;
    TString Filter;
    bool FetchValues = true;
    bool FetchTimestamps = false;
    std::optional<int> Offset;
    std::optional<int> Limit;
    std::optional<TString> ContinuationToken;
    std::optional<TString> Index;
    std::optional<TObjectOrderBy> OrderBy;
    bool FetchRootObject = false;
    TTransactionId TimestampByTransactionId = TTransactionId();
    bool FetchPerformanceStatistics = false;
    std::optional<bool> AllowFullScan;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TSelectObjectsOptions& options,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

template <typename TAttributeSelectorProto>
void ToProto(
    TAttributeSelectorProto* protoSelector,
    const TAttributeSelector& selector)
{
    for (const auto& attributePath : selector) {
        protoSelector->add_paths(attributePath);
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TGetObjectOptions
{
    NObjects::TTimestamp Timestamp = NObjects::NullTimestamp;
    EPayloadFormat Format = EPayloadFormat::Yson;
    bool IgnoreNonexistent = false;
    bool SkipNonexistent = false;
    bool FetchValues = true;
    bool FetchTimestamps = false;
    bool FetchRootObject = false;
    TTransactionId TimestampByTransactionId = TTransactionId();
    TTransactionId TransactionId = TTransactionId();
    bool FetchPerformanceStatistics = false;
    std::optional<bool> AllowFullScan;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TGetObjectOptions& options,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TSetUpdate
{
    NYPath::TYPath Path;
    TPayload Payload;
    bool Recursive = false;
    std::optional<bool> SharedWrite;
    EAggregateMode AggregateMode = EAggregateMode::Unspecified;

    Y_SAVELOAD_DEFINE(Path, Payload, Recursive, SharedWrite, AggregateMode);
};

void FormatValue(
    TStringBuilderBase* builder,
    const TSetUpdate& setUpdate,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TSetRootUpdate
{
    std::vector<NYPath::TYPath> Paths;
    TPayload Payload;
    bool Recursive = false;
    std::optional<bool> SharedWrite;
    EAggregateMode AggregateMode = EAggregateMode::Unspecified;

    Y_SAVELOAD_DEFINE(Paths, Payload, Recursive, SharedWrite, AggregateMode);
};

void FormatValue(
    TStringBuilderBase* builder,
    const TSetRootUpdate& setUpdate,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TRemoveUpdate
{
    NYPath::TYPath Path;
    bool Force = false;

    Y_SAVELOAD_DEFINE(Path, Force);
};

void FormatValue(
    TStringBuilderBase* builder,
    const TRemoveUpdate& removeUpdate,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TLockUpdate
{
    NYPath::TYPath Path;
    NTableClient::ELockType LockType;

    Y_SAVELOAD_DEFINE(Path, LockType);
};

void FormatValue(
    TStringBuilderBase* builder,
    const TLockUpdate& lockUpdate,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TMethodCall
{
    NYPath::TYPath Path;
    TPayload Payload;

    Y_SAVELOAD_DEFINE(Path, Payload);
};

void FormatValue(
    TStringBuilderBase* builder,
    const TMethodCall& methodCall,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TAttributeTimestampPrerequisite
{
    NYPath::TYPath Path;
    NObjects::TTimestamp Timestamp;

    Y_SAVELOAD_DEFINE(Path, Timestamp);
};

void FormatValue(
    TStringBuilderBase* builder,
    const TAttributeTimestampPrerequisite& attributeTimestampPrerequisite,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TUpdateObjectsSubrequest
{
    TObjectIdentity ObjectIdentity;
    NObjects::TObjectTypeValue ObjectType = NObjects::TObjectTypeValues::Null;
    std::vector<TUpdate> Updates;
    std::vector<TAttributeTimestampPrerequisite> AttributeTimestampPrerequisites;

    Y_SAVELOAD_DEFINE(ObjectIdentity, ObjectType, Updates, AttributeTimestampPrerequisites);
};

void FormatValue(
    TStringBuilderBase* builder,
    const TUpdateObjectsSubrequest& updateObjectsSubrequest,
    NObjects::IConstObjectTypeRegistryPtr objectTypeRegistry,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TUpdateObjectOptions
{
    TTransactionId TransactionId = TTransactionId();
    bool IgnoreNonexistent = false;
    bool FetchPerformanceStatistics = false;
    std::optional<bool> AllowFullScan;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TUpdateObjectOptions& options,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TUpdateIfExisting
{
    std::vector<TUpdate> Updates;
    std::vector<TAttributeTimestampPrerequisite> AttributeTimestampPrerequisites;

    Y_SAVELOAD_DEFINE(Updates, AttributeTimestampPrerequisites);
};

void FormatValue(
    TStringBuilderBase* builder,
    const TUpdateIfExisting& request,
    TStringBuf format);

TString ConvertToString(const std::optional<TUpdateIfExisting>& request, bool writePayload);

////////////////////////////////////////////////////////////////////////////////

struct TCreateObjectsSubrequest
{
    NObjects::TObjectTypeValue ObjectType = NObjects::TObjectTypeValues::Null;
    TPayload AttributesPayload = TNullPayload();
    std::optional<TUpdateIfExisting> UpdateIfExisting = std::nullopt;

    Y_SAVELOAD_DEFINE(ObjectType, AttributesPayload, UpdateIfExisting);
};

void FormatValue(
    TStringBuilderBase* builder,
    const TCreateObjectsSubrequest& request,
    NObjects::IConstObjectTypeRegistryPtr objectTypeRegistry,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TCreateObjectOptions
{
    TTransactionId TransactionId = TTransactionId();
    EPayloadFormat Format = EPayloadFormat::None;
    bool FetchPerformanceStatistics = false;
    std::optional<bool> AllowFullScan;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TCreateObjectOptions& request,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TRemoveObjectsSubrequest
{
    TObjectIdentity ObjectIdentity;
    NObjects::TObjectTypeValue ObjectType = NObjects::TObjectTypeValues::Null;

    Y_SAVELOAD_DEFINE(ObjectIdentity, ObjectType);
};

void FormatValue(
    TStringBuilderBase* builder,
    const TRemoveObjectsSubrequest& removeObjectsSubrequest,
    NObjects::IConstObjectTypeRegistryPtr objectTypeRegistry,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TRemoveObjectOptions
{
    TTransactionId TransactionId = TTransactionId();
    bool IgnoreNonexistent = false;
    bool FetchPerformanceStatistics = false;
    std::optional<bool> AllowFullScan;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TRemoveObjectOptions& request,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TMutatingTransactionOptions
{
    TTransactionContext TransactionContext;
    bool SkipWatchLog = false;
    bool SkipHistory = false;
    bool SkipRevisionBump = false;
    std::optional<bool> AllowRemovalWithNonEmptyReference;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TMutatingTransactionOptions& MutatingTransactionOptions,
    TStringBuf format);

template <typename TMutatingTransactionOptionsProto>
void ToProto(TMutatingTransactionOptionsProto* protoOptions, const TMutatingTransactionOptions& options);

struct TStartTransactionOptions
{
    NTransactionClient::TTimestamp StartTimestamp = NTransactionClient::NullTimestamp;
    NTransactionClient::TTransactionId UnderlyingTransactionId;
    TString UnderlyingTransactionAddress;
    TMutatingTransactionOptions MutatingTransactionOptions;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TStartTransactionOptions& startTransactionOptions,
    TStringBuf format);

struct TCommitTransactionOptions
{
    bool FetchPerformanceStatistics = false;
    std::optional<bool> AllowFullScan;
    TTransactionContext TransactionContext;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TCommitTransactionOptions& startTransactionOptions,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TWatchObjectsOptions
{
    // Either StartTimestamp or ContinuationToken should be defined.
    NObjects::TTimestamp StartTimestamp = NObjects::NullTimestamp;
    bool StartFromEarliestOffset = false;
    std::optional<TString> ContinuationToken;
    NObjects::TTimestamp Timestamp = NObjects::NullTimestamp;
    std::optional<i64> EventCountLimit;
    std::optional<TDuration> TimeLimit;
    std::optional<TDuration> ReadTimeLimit;
    std::optional<TString> Filter;
    std::optional<TAttributeSelector> Selector;
    std::optional<std::vector<int>> Tablets;
    EPayloadFormat Format = EPayloadFormat::None;
    bool SkipTrimmed = false;
    bool FetchPerformanceStatistics = false;
    bool FetchChangedAttributes = false;
    bool FetchEventIndex = false;
    std::optional<bool> AllowFullScan;
    TString WatchLog;
    std::vector<NObjects::TTag> RequiredTags;
    std::vector<NObjects::TTag> ExcludedTags;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TWatchObjectsOptions& watchObjectsOptions,
    TStringBuf format);

TString ConvertToString(
    const TWatchObjectsOptions& options,
    NObjects::IConstTagsRegistryPtr tagsRegistry);

////////////////////////////////////////////////////////////////////////////////

struct TSelectObjectHistoryOptions
{
    struct TTimeInterval
    {
        std::optional<TInstant> Begin;
        std::optional<TInstant> End;
    };

    struct TTimestampInterval
    {
        std::optional<NTransactionClient::TTimestamp> Begin;
        std::optional<NTransactionClient::TTimestamp> End;
    };

    std::optional<i32> Limit;
    std::optional<TString> ContinuationToken;
    TTimeInterval TimeInterval;
    TTimestampInterval TimestampInterval;
    bool DescendingTimeOrder = false;
    bool Distinct = false;
    bool FetchRootObject = false;
    EPayloadFormat Format = EPayloadFormat::None;
    bool FetchPerformanceStatistics = false;
    std::optional<bool> AllowFullScan;
    NObjects::ESelectObjectHistoryIndexMode IndexMode = NObjects::ESelectObjectHistoryIndexMode::Default;
    bool AllowTimeModeConversion = false;
    TString Filter;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TSelectObjectHistoryOptions& selectObjectHistoryOptions,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

template <class TUpdatesProto>
void FillAttributeTimestampPrerequisites(
    TUpdatesProto* updatesProto,
    std::vector<TAttributeTimestampPrerequisite> attributeTimestampPrerequisites);

template <class TUpdatesProto>
void ToProto(
    TUpdatesProto* updatesProto,
    std::vector<TUpdate> updates);

template <class TProtoRequest, class TCommonOptions>
void FillCommonOptions(TProtoRequest protoRequest, const TCommonOptions& options);

template <class EObjectType, class TUpdateRequest>
void FillUpdateObjectRequestAttributes(
    NNative::TObjectIdentity objectIdentity,
    NObjects::TObjectTypeValue objectType,
    std::vector<NNative::TUpdate> updates,
    std::vector<NNative::TAttributeTimestampPrerequisite> attributeTimestampPrerequisites,
    TUpdateRequest* req);

template <class TGetRequest>
void FillGetObjectRequestOptions(
    const NNative::TGetObjectOptions& options,
    TGetRequest* req);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative

#define REQUEST_INL_H_
#include "request-inl.h"
#undef REQUEST_INL_H_
