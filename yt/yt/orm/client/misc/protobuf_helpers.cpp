#include "protobuf_helpers.h"

#include <yt/yt/core/json/json_parser.h>
#include <yt/yt/core/json/json_writer.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

namespace NYT::NOrm::NClient {

using NYTree::NProto::TAttributeDictionary;

////////////////////////////////////////////////////////////////////////////////

TAttributeDictionary JsonToAttributeDictionary(
    const ::NJson::TJsonValue& jsonValue)
{
    TStringStream jsonStream;
    ::NJson::WriteJson(&jsonStream, &jsonValue);
    jsonStream.Finish();

    TStringStream ysonStream;
    NYson::TYsonWriter ysonWriter(&ysonStream, NYson::EYsonFormat::Binary);
    NJson::ParseJson(&jsonStream, &ysonWriter);
    ysonStream.Finish();

    return NYTree::ConvertTo<TAttributeDictionary>(NYTree::ConvertToNode(&ysonStream));
}

::NJson::TJsonValue JsonFromAttributeDictionary(
    const TAttributeDictionary& attributeDictionary)
{
    TStringStream jsonStream;
    auto jsonConsumer = NJson::CreateJsonConsumer(&jsonStream);
    NYson::WriteProtobufMessage(jsonConsumer.get(), attributeDictionary);
    jsonConsumer->Flush();
    jsonStream.Finish();

    return ::NJson::ReadJsonTree(&jsonStream);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient
