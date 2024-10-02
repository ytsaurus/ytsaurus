#include "payload.h"

#include <yt/yt/core/misc/string_builder.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NOrm::NClient::NNative {

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

bool operator==(const TNullPayload&, const TNullPayload&)
{
    return true;
}

bool operator==(const TYsonPayload& lhs, const TYsonPayload& rhs)
{
    return lhs.Yson == rhs.Yson;
}

bool operator==(const TProtobufPayload& lhs, const TProtobufPayload& rhs)
{
    return lhs.Protobuf == rhs.Protobuf;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative
