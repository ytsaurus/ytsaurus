#pragma once

#include "public.h"

#include <yt/yt/core/yson/string.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NOrm::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

struct TNullPayload
{
    Y_SAVELOAD_DEFINE();
};

struct TYsonPayload
{
    NYT::NYson::TYsonString Yson;

    Y_SAVELOAD_DEFINE(Yson);
};

struct TProtobufPayload
{
    TString Protobuf;

    Y_SAVELOAD_DEFINE(Protobuf);
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

template <typename TPayloadProto>
void FromProto(
    TPayload* payload,
    const TPayloadProto& protoPayload)
{
    if (protoPayload.has_yson()) {
        *payload = TYsonPayload{NYson::TYsonString(protoPayload.yson())};
    } else if (protoPayload.has_protobuf()) {
        *payload = TProtobufPayload{protoPayload.protobuf()};
    } else {
        *payload = TNullPayload();
    }
}

template <typename TPayloadProto>
void ToProto(
    TPayloadProto* protoPayload,
    const TPayload& payload)
{
    Visit(payload,
        [&] (const TNullPayload& /*null*/) {
            protoPayload->set_null(true);
        },
        [&] (const TYsonPayload& yson) {
            protoPayload->set_yson(yson.Yson.ToString());
        },
        [&] (const TProtobufPayload& protobuf) {
            protoPayload->set_protobuf(protobuf.Protobuf);
        });
}

bool operator==(const TNullPayload&, const TNullPayload&);
bool operator==(const TYsonPayload& lhs, const TYsonPayload& rhs);
bool operator==(const TProtobufPayload& lhs, const TProtobufPayload& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative
