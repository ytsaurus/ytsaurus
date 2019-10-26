#include "payload.h"

#include <yt/core/misc/format.h>
#include <yt/core/misc/string_builder.h>
#include <yt/core/misc/variant.h>
#include <yt/core/ytree/convert.h>

namespace NYP::NClient::NApi::NNative {

using namespace NYT::NYTree;
using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TNullPayload& /*null*/,
    TStringBuf /*format*/)
{
    builder->AppendString("{}");
}

void FormatValue(
    TStringBuilderBase* builder,
    const TYsonPayload& yson,
    TStringBuf /*format*/)
{
    builder->AppendFormat("{Yson: %Qv}",
        yson.Yson);
}

void FormatValue(
    TStringBuilderBase* builder,
    const TProtobufPayload& protobuf,
    TStringBuf /*format*/)
{
    builder->AppendFormat("{Protobuf: %Qv}",
        protobuf.Protobuf);
}

void FormatValue(
    TStringBuilderBase* builder,
    const TPayload& payload,
    TStringBuf format)
{
    Visit(payload, [&] (const auto& concretePayload) {
        FormatValue(builder, concretePayload, format);
    });
}

////////////////////////////////////////////////////////////////////////////////

void FromProto(
    TPayload* payload,
    const NProto::TPayload& protoPayload)
{
    if (protoPayload.has_yson()) {
        *payload = TYsonPayload{NYson::TYsonString(protoPayload.yson())};
    } else if (protoPayload.has_protobuf()) {
        *payload = TProtobufPayload{protoPayload.protobuf()};
    } else {
        *payload = TNullPayload();
    }
}

void ToProto(
    NProto::TPayload* protoPayload,
    const TPayload& payload)
{
    Visit(payload,
        [&] (const TNullPayload& /*null*/) {
            protoPayload->set_null(true);
        },
        [&] (const TYsonPayload& yson) {
            protoPayload->set_yson(yson.Yson.GetData());
        },
        [&] (const TProtobufPayload& protobuf) {
            protoPayload->set_protobuf(protobuf.Protobuf);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NClient::NApi::NNative
