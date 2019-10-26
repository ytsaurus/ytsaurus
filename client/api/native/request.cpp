#include "request.h"

#include <yt/core/misc/format.h>
#include <yt/core/misc/string_builder.h>

namespace NYP::NClient::NApi::NNative {

using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TSelectObjectsOptions& options,
    TStringBuf /*format*/)
{
    builder->AppendFormat("{"
        "Timestamp: %llx, "
        "Format: %v, "
        "Filter: %Qv, "
        "FetchValues: %v, "
        "FetchTimestamps: %v, "
        "Offset: %v, "
        "Limit: %v, "
        "ContinuationToken: %Qv}",
        options.Timestamp,
        options.Format,
        options.Filter,
        options.FetchValues,
        options.FetchTimestamps,
        options.Offset,
        options.Limit,
        options.ContinuationToken);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TAttributeSelector* protoSelector,
    const TAttributeSelector& selector)
{
    for (const auto& attributePath : selector) {
        protoSelector->add_paths(attributePath);
    }
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TGetObjectOptions& options,
    TStringBuf /*format*/)
{
    builder->AppendFormat("{"
        "Timestamp: %llx, "
        "Format: %v, "
        "IgnoreNonexistent: %v, "
        "FetchValues: %v, "
        "FetchTimestamps: %v}",
        options.Timestamp,
        options.Format,
        options.IgnoreNonexistent,
        options.FetchValues,
        options.FetchTimestamps);
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TSetUpdate& setUpdate,
    TStringBuf /*format*/)
{
    builder->AppendFormat("{Path: %v, Payload: %v, Recursive: %v}",
        setUpdate.Path,
        setUpdate.Payload,
        setUpdate.Recursive);
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TRemoveUpdate& removeUpdate,
    TStringBuf /*format*/)
{
    builder->AppendFormat("{Path: %v}",
        removeUpdate.Path);
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TAttributeTimestampPrerequisite& attributeTimestampPrerequisite,
    TStringBuf /*format*/)
{
    builder->AppendFormat("{Path: %v, Timestamp: %llx}",
        attributeTimestampPrerequisite.Path,
        attributeTimestampPrerequisite.Timestamp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NClient::NApi::NNative
