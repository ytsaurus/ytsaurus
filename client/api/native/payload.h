#pragma once

#include "public.h"

#include <yp/client/api/proto/object_service.pb.h>

#include <yt/core/yson/string.h>

namespace NYP::NClient::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

struct TNullPayload
{ };

struct TYsonPayload
{
    NYT::NYson::TYsonString Yson;
};

struct TProtobufPayload
{
    TString Protobuf;
};

void FormatValue(
    NYT::TStringBuilderBase* builder,
    const TNullPayload& null,
    TStringBuf format);

void FormatValue(
    NYT::TStringBuilderBase* builder,
    const TYsonPayload& yson,
    TStringBuf format);

void FormatValue(
    NYT::TStringBuilderBase* builder,
    const TProtobufPayload& protobuf,
    TStringBuf format);

void FormatValue(
    NYT::TStringBuilderBase* builder,
    const TPayload& payload,
    TStringBuf /*format*/);

////////////////////////////////////////////////////////////////////////////////

void FromProto(
    TPayload* payload,
    const NProto::TPayload& protoPayload);

void ToProto(
    NProto::TPayload* protoPayload,
    const TPayload& payload);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NClient::NApi::NNative
