#include "request.h"

#include <yt/yt/orm/client/objects/registry.h>
#include <yt/yt/orm/client/objects/tags.h>
#include <yt/yt/orm/client/objects/type.h>

#include <yt/yt/core/misc/string_builder.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NOrm::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TObjectOrderByExpression& orderByExpression,
    TStringBuf /*format*/)
{
    TDelimitedStringBuilderWrapper wrapper(builder, ", ");

    builder->AppendChar('{');
    wrapper->AppendFormat("Expression: %v", orderByExpression.Expression);
    wrapper->AppendFormat("Descending: %v", orderByExpression.Descending);
    builder->AppendChar('}');
}

////////////////////////////////////////////////////////////////////////////////

void TAdaptiveBatchSizeOptions::Validate() const
{
    auto validateField = [] (auto fieldName, auto fieldValue, auto minFieldValue) {
        THROW_ERROR_EXCEPTION_UNLESS(
            fieldValue >= minFieldValue,
            "%v must be at least %v, got %v",
            fieldName,
            minFieldValue,
            fieldValue);
    };

    validateField("Increasing additive", IncreasingAdditive, 0);
    validateField("Decreasing divisor", DecreasingDivisor, 1);
    validateField("Max batch size", MaxBatchSize, 1);
    validateField("Min batch size", MaxBatchSize, 1);
    if (MaxConsecutiveRetryCount.has_value()) {
        validateField("Max consecutive retry count", *MaxConsecutiveRetryCount, 0);
    }

    THROW_ERROR_EXCEPTION_UNLESS(
        MaxBatchSize >= MinBatchSize,
        "Max batch size must be greater than or equal to min batch size, got %v and %v",
        MaxBatchSize,
        MinBatchSize);
}

void FormatValue(
    TStringBuilderBase* builder,
    const TAdaptiveBatchSizeOptions& options,
    TStringBuf /*format*/)
{
    TDelimitedStringBuilderWrapper wrapper(builder, ", ");

    builder->AppendChar('{');
    wrapper->AppendFormat("IncreasingAdditive: %v", options.IncreasingAdditive);
    wrapper->AppendFormat("DecreasingDivisor: %v", options.DecreasingDivisor);
    wrapper->AppendFormat("MaxBatchSize: %v", options.MaxBatchSize);
    wrapper->AppendFormat("MinBatchSize: %v", options.MinBatchSize);
    wrapper->AppendFormat("MaxConsecutiveRetryCount: %v", options.MaxConsecutiveRetryCount);
    builder->AppendChar('}');
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TSelectObjectsOptions& options,
    TStringBuf /*format*/)
{
    TDelimitedStringBuilderWrapper wrapper(builder, ", ");

    builder->AppendChar('{');
    wrapper->AppendFormat("Timestamp: %v", options.Timestamp);
    wrapper->AppendFormat("Format: %v", options.Format);
    wrapper->AppendFormat("Filter: %v", options.Filter);
    wrapper->AppendFormat("FetchValues: %v", options.FetchValues);
    wrapper->AppendFormat("FetchTimestamps: %v", options.FetchTimestamps);
    wrapper->AppendFormat("Offset: %v", options.Offset);
    wrapper->AppendFormat("Limit: %v", options.Limit);
    wrapper->AppendFormat("ContinuationToken: %v", options.ContinuationToken);
    wrapper->AppendFormat("OrderBy: %v", options.OrderBy);
    wrapper->AppendFormat("FetchRootObject: %v", options.FetchRootObject);
    wrapper->AppendFormat("Index: %v", options.Index);
    wrapper->AppendFormat("TimestampByTransactionId: %v", options.TimestampByTransactionId);
    wrapper->AppendFormat("FetchPerformanceStatistics: %v", options.FetchPerformanceStatistics);
    wrapper->AppendFormat("AllowFullScan: %v", options.AllowFullScan);
    builder->AppendChar('}');
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TGetObjectOptions& options,
    TStringBuf /*format*/)
{
    TDelimitedStringBuilderWrapper wrapper(builder, ", ");

    builder->AppendChar('{');
    wrapper->AppendFormat("Timestamp: %v", options.Timestamp);
    wrapper->AppendFormat("Format: %v", options.Format);
    wrapper->AppendFormat("IgnoreNonexistent: %v", options.IgnoreNonexistent);
    wrapper->AppendFormat("SkipNonexistent: %v", options.SkipNonexistent);
    wrapper->AppendFormat("FetchValues: %v", options.FetchValues);
    wrapper->AppendFormat("FetchTimestamps: %v", options.FetchTimestamps);
    wrapper->AppendFormat("FetchRootObject: %v", options.FetchRootObject);
    wrapper->AppendFormat("TimestampByTransactionId: %v", options.TimestampByTransactionId);
    wrapper->AppendFormat("TransactionId: %v", options.TransactionId);
    wrapper->AppendFormat("FetchPerformanceStatistics: %v", options.FetchPerformanceStatistics);
    wrapper->AppendFormat("AllowFullScan: %v", options.AllowFullScan);
    builder->AppendChar('}');
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TSetUpdate& setUpdate,
    TStringBuf format)
{
    builder->AppendFormat("{Path: %v, Payload: %v, Recursive: %v, SharedWrite: %v, AggregateMode: %v}",
        setUpdate.Path,
        format == OmitPayloadLogFormat ? TPayload() : setUpdate.Payload,
        setUpdate.Recursive,
        setUpdate.SharedWrite,
        setUpdate.AggregateMode);
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TSetRootUpdate& setUpdate,
    TStringBuf format)
{
    builder->AppendFormat("{Paths: %v, Payload: %v, Recursive: %v, SharedWrite: %v, AggregateMode: %v}",
        setUpdate.Paths,
        format == OmitPayloadLogFormat ? TPayload() : setUpdate.Payload,
        setUpdate.Recursive,
        setUpdate.SharedWrite,
        setUpdate.AggregateMode);
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TRemoveUpdate& removeUpdate,
    TStringBuf /*format*/)
{
    builder->AppendFormat("{Path: %v, Force: %v}",
        removeUpdate.Path,
        removeUpdate.Force);
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TLockUpdate& lockUpdate,
    TStringBuf /*format*/)
{
    builder->AppendFormat("{Path: %v, LockType: %v}",
        lockUpdate.Path,
        lockUpdate.LockType);
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TMethodCall& methodCall,
    TStringBuf format)
{
    builder->AppendFormat("{Path: %v, Payload: %v}",
        methodCall.Path,
        format == OmitPayloadLogFormat ? TPayload() : methodCall.Payload);
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TAttributeTimestampPrerequisite& attributeTimestampPrerequisite,
    TStringBuf /*format*/)
{
    builder->AppendFormat("{Path: %v, Timestamp: %v}",
        attributeTimestampPrerequisite.Path,
        attributeTimestampPrerequisite.Timestamp);
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TUpdateObjectsSubrequest& updateObjectsSubrequest,
    NObjects::IConstObjectTypeRegistryPtr objectTypeRegistry,
    TStringBuf format)
{
    TDelimitedStringBuilderWrapper wrapper(builder, ", ");

    builder->AppendChar('{');
    wrapper->AppendFormat("ObjectIdentity: %v", updateObjectsSubrequest.ObjectIdentity);
    wrapper->AppendFormat("ObjectType: %v", objectTypeRegistry->FormatTypeValue(updateObjectsSubrequest.ObjectType));
    wrapper->AppendFormat("Updates: %v",
        MakeFormattableView(updateObjectsSubrequest.Updates,
            [format] (TStringBuilderBase* builder, const auto& update) {
                FormatValue(builder, update, format);
            }));
    wrapper->AppendFormat("AttributeTimestampPrerequisites: %v", updateObjectsSubrequest.AttributeTimestampPrerequisites);
    builder->AppendChar('}');
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TUpdateObjectOptions& options,
    TStringBuf /*format*/)
{
    TDelimitedStringBuilderWrapper wrapper(builder, ", ");

    builder->AppendChar('{');
    wrapper->AppendFormat("TransactionId: %v", options.TransactionId);
    wrapper->AppendFormat("FetchPerformanceStatistics: %v", options.FetchPerformanceStatistics);
    wrapper->AppendFormat("IgnoreNonexistent: %v", options.IgnoreNonexistent);
    wrapper->AppendFormat("AllowFullScan: %v", options.AllowFullScan);
    builder->AppendChar('}');
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TUpdateIfExisting& request,
    TStringBuf format)
{
    TDelimitedStringBuilderWrapper wrapper(builder, ", ");

    builder->AppendChar('{');
    wrapper->AppendFormat("Updates: %v",
        MakeFormattableView(request.Updates,
        [format] (auto* builder, const auto& update) {
            FormatValue(builder, update, format);
        }));
    wrapper->AppendFormat("AttributeTimestampPrerequisites: %v", request.AttributeTimestampPrerequisites);
    builder->AppendChar('}');
}

TString ConvertToString(const std::optional<TUpdateIfExisting>& request, bool writePayload)
{
    TStringBuilder builder;
    FormatValue(
        &builder,
        request,
        /*format*/ writePayload ? "" : OmitPayloadLogFormat);
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TCreateObjectsSubrequest& request,
    NObjects::IConstObjectTypeRegistryPtr objectTypeRegistry,
    TStringBuf format)
{
    TDelimitedStringBuilderWrapper wrapper(builder, ", ");

    builder->AppendChar('{');
    wrapper->AppendFormat("ObjectType: %v", objectTypeRegistry->FormatTypeValue(request.ObjectType));
    wrapper->AppendFormat("AttributesPayload: %v",
        format == OmitPayloadLogFormat ? TPayload() : request.AttributesPayload);
    if (!request.UpdateIfExisting) {
        wrapper->AppendFormat("UpdateIfExists: %v", request.UpdateIfExisting);
    } else {
        wrapper->AppendString("UpdateIfExists: ");
        FormatValue(builder, *request.UpdateIfExisting, format);
    }
    builder->AppendChar('}');
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TCreateObjectOptions& options,
    TStringBuf /*format*/)
{
    TDelimitedStringBuilderWrapper wrapper(builder, ", ");

    builder->AppendChar('{');
    wrapper->AppendFormat("TransactionId: %v", options.TransactionId);
    wrapper->AppendFormat("Format: %v", options.Format);
    wrapper->AppendFormat("FetchPerformanceStatistics: %v", options.FetchPerformanceStatistics);
    wrapper->AppendFormat("AllowFullScan: %v", options.AllowFullScan);
    builder->AppendChar('}');
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TRemoveObjectsSubrequest& removeObjectsSubrequest,
    NObjects::IConstObjectTypeRegistryPtr objectTypeRegistry,
    TStringBuf /*format*/)
{
    TDelimitedStringBuilderWrapper wrapper(builder, ", ");

    builder->AppendChar('{');
    wrapper->AppendFormat("ObjectIdentity: %v", removeObjectsSubrequest.ObjectIdentity);
    wrapper->AppendFormat("ObjectType: %v", objectTypeRegistry->FormatTypeValue(removeObjectsSubrequest.ObjectType));
    builder->AppendChar('}');
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TRemoveObjectOptions& options,
    TStringBuf /*format*/)
{
    TDelimitedStringBuilderWrapper wrapper(builder, ", ");

    builder->AppendChar('{');
    wrapper->AppendFormat("TransactionId: %v", options.TransactionId);
    wrapper->AppendFormat("IgnoreNonexistent: %v", options.IgnoreNonexistent);
    wrapper->AppendFormat("FetchPerformanceStatistics: %v", options.FetchPerformanceStatistics);
    wrapper->AppendFormat("AllowFullScan: %v", options.AllowFullScan);
    builder->AppendChar('}');
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TMutatingTransactionOptions& options,
    TStringBuf /*format*/)
{
    TDelimitedStringBuilderWrapper wrapper(builder, ", ");

    builder->AppendChar('{');
    wrapper->AppendFormat("TransactionContext: %v", options.TransactionContext);
    wrapper->AppendFormat("SkipWatchLog: %v", options.SkipWatchLog);
    wrapper->AppendFormat("SkipHistory: %v", options.SkipHistory);
    wrapper->AppendFormat("SkipRevisionBump: %v", options.SkipRevisionBump);
    wrapper->AppendFormat("AllowRemovalWithNonEmptyReferenc: %v", options.AllowRemovalWithNonEmptyReference);
    builder->AppendChar('}');
}

void FormatValue(
    TStringBuilderBase* builder,
    const TStartTransactionOptions& options,
    TStringBuf /*format*/)
{
    TDelimitedStringBuilderWrapper wrapper(builder, ", ");

    builder->AppendChar('{');
    wrapper->AppendFormat("StartTimestamp: %v", options.StartTimestamp);
    wrapper->AppendFormat("UnderlyingTransactionId: %v", options.UnderlyingTransactionId);
    wrapper->AppendFormat("UnderlyingTransactionAddress: %v", options.UnderlyingTransactionAddress);
    wrapper->AppendFormat("MutatingTransactionOption: %v", options.MutatingTransactionOptions);
    builder->AppendChar('}');
}

void FormatValue(
    TStringBuilderBase* builder,
    const TCommitTransactionOptions& options,
    TStringBuf /*format*/)
{
    TDelimitedStringBuilderWrapper wrapper(builder, ", ");

    builder->AppendChar('{');
    wrapper->AppendFormat("FetchPerformanceStatistics: %v", options.FetchPerformanceStatistics);
    wrapper->AppendFormat("AllowFullScan: %v", options.AllowFullScan);
    wrapper->AppendFormat("TransactionContext: %v", options.TransactionContext);
    builder->AppendChar('}');
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TWatchObjectsOptions& options,
    NObjects::IConstTagsRegistryPtr tagsRegistry,
    TStringBuf /*format*/)
{
    TDelimitedStringBuilderWrapper wrapper(builder, ", ");

    builder->AppendChar('{');
    wrapper->AppendFormat("StartTimestamp: %v", options.StartTimestamp);
    wrapper->AppendFormat("StartFromEarliestTimestamp: %v", options.StartFromEarliestOffset);
    wrapper->AppendFormat("ContinuationToken: %v", options.ContinuationToken);
    wrapper->AppendFormat("Timestamp: %v", options.Timestamp);
    wrapper->AppendFormat("EventCountLimit: %v", options.EventCountLimit);
    wrapper->AppendFormat("TimeLimit: %v", options.TimeLimit);
    wrapper->AppendFormat("ReadTimeLimit: %v", options.ReadTimeLimit);
    wrapper->AppendFormat("Filter: %v", options.Filter);
    wrapper->AppendFormat("Selector: %v", options.Selector);
    wrapper->AppendFormat("Format: %v", options.Format);
    wrapper->AppendFormat("FetchPerformanceStatistics: %v", options.FetchPerformanceStatistics);
    wrapper->AppendFormat("AllowFullScan: %v", options.AllowFullScan);
    wrapper->AppendFormat("WatchLog: %v", options.WatchLog);
    wrapper->AppendFormat("RequiredTags: %v", tagsRegistry->MakeHumanReadableTagsList(options.RequiredTags));
    wrapper->AppendFormat("ExcludedTags: %v", tagsRegistry->MakeHumanReadableTagsList(options.ExcludedTags));
    builder->AppendChar('}');
}

TString ConvertToString(
    const TWatchObjectsOptions& options,
    NObjects::IConstTagsRegistryPtr tagsRegistry)
{
    TStringBuilder builder;
    FormatValue(
        &builder,
        options,
        tagsRegistry,
        "");
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TSelectObjectHistoryOptions& options,
    TStringBuf /*format*/)
{
    TDelimitedStringBuilderWrapper wrapper(builder, ", ");

    builder->AppendChar('{');
    wrapper->AppendFormat("Limit: %v", options.Limit);
    wrapper->AppendFormat("ContinuationToken: %v", options.ContinuationToken);
    wrapper->AppendFormat("TimeInterval: {Begin: %v, End: %v}",
        options.TimeInterval.Begin,
        options.TimeInterval.End);
    wrapper->AppendFormat("TimestampInterval: {Begin: %v, End: %v}",
        options.TimeInterval.Begin,
        options.TimeInterval.End);
    wrapper->AppendFormat("DescendingTimeOrder: %v", options.DescendingTimeOrder);
    wrapper->AppendFormat("Distinct: %v", options.Distinct);
    wrapper->AppendFormat("FetchRootObject: %v", options.FetchRootObject);
    wrapper->AppendFormat("Format: %v", options.Format);
    wrapper->AppendFormat("FetchPerformanceStatistics: %v", options.FetchPerformanceStatistics);
    wrapper->AppendFormat("AllowFullScan: %v", options.AllowFullScan);
    wrapper->AppendFormat("IndexMode: %v", options.IndexMode);
    wrapper->AppendFormat("AllowTimeModeConversion: %v", options.AllowTimeModeConversion);
    wrapper->AppendFormat("Filter: %v", options.Filter);
    builder->AppendChar('}');
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative
