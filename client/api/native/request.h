#pragma once

#include "public.h"

#include "payload.h"

namespace NYP::NClient::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

struct TSelectObjectsOptions
{
    TTimestamp Timestamp = NullTimestamp;
    EPayloadFormat Format = EPayloadFormat::Yson;
    TString Filter;
    bool FetchValues = true;
    bool FetchTimestamps = false;
    std::optional<int> Offset;
    std::optional<int> Limit;
    std::optional<TString> ContinuationToken;
};

void FormatValue(
    NYT::TStringBuilderBase* builder,
    const TSelectObjectsOptions& options,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TAttributeSelector* protoSelector,
    const TAttributeSelector& selector);

////////////////////////////////////////////////////////////////////////////////

struct TGetObjectOptions
{
    TTimestamp Timestamp = NullTimestamp;
    EPayloadFormat Format = EPayloadFormat::Yson;
    bool IgnoreNonexistent = false;
    bool FetchValues = true;
    bool FetchTimestamps = false;
};

void FormatValue(
    NYT::TStringBuilderBase* builder,
    const TGetObjectOptions& options,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TSetUpdate
{
    NYT::NYPath::TYPath Path;
    TPayload Payload;
    bool Recursive = false;
};

void FormatValue(
    NYT::TStringBuilderBase* builder,
    const TSetUpdate& setUpdate,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TRemoveUpdate
{
    NYT::NYPath::TYPath Path;
};

void FormatValue(
    NYT::TStringBuilderBase* builder,
    const TRemoveUpdate& removeUpdate,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TAttributeTimestampPrerequisite
{
    NYT::NYPath::TYPath Path;
    TTimestamp Timestamp;
};

void FormatValue(
    NYT::TStringBuilderBase* builder,
    const TAttributeTimestampPrerequisite& attributeTimestampPrerequisite,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NClient::NApi::NNative
