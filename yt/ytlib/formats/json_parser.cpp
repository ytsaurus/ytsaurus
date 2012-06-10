#include "stdafx.h"
#include "json_parser.h"

#include <library/json/json_reader.h>

#include <util/string/base64.h>

namespace NYT {
namespace NFormats {

using namespace NJson;

////////////////////////////////////////////////////////////////////////////////

TJsonParser::TJsonParser(NYTree::IYsonConsumer* consumer, TJsonFormatConfigPtr config)
    : Config(config)
    , Consumer(consumer)
{
    if (!Config) {
        Config = New<TJsonFormatConfig>();
    }
}

void TJsonParser::Read(const TStringBuf& data)
{
    Stream.Write(data);
}

void TJsonParser::Finish()
{
    Parse(&Stream);
}

void TJsonParser::Parse(TInputStream* input)
{
    TJsonValue jsonValue;
    ReadJsonTree(input, &jsonValue);
    VisitAny(jsonValue);
}

void TJsonParser::VisitAny(const TJsonValue& value)
{
    switch (value.GetType()) {
        case JSON_ARRAY:
            VisitArray(value.GetArray());
            break;
        case JSON_MAP:
            VisitMap(value.GetMap());
            break;
        case JSON_INTEGER:
            Consumer->OnIntegerScalar(value.GetInteger());
            break;
        case JSON_DOUBLE:
            Consumer->OnDoubleScalar(value.GetDouble());
            break;
        case JSON_NULL:
            Consumer->OnEntity();
            break;
        case JSON_STRING:
            Consumer->OnStringScalar(DecodeString(value.GetString()));
            break;
        case JSON_BOOLEAN:
            ythrow yexception() << "Json boolean are not supported";
            break;
        case JSON_UNDEFINED:
        default:
            YUNREACHABLE();
            break;
    }
}

void TJsonParser::VisitMapItems(const TJsonValue::TMap& map)
{
    FOREACH (const auto& pair, map) {
        Consumer->OnKeyedItem(DecodeString(pair.first));
        VisitAny(pair.second);
    }
}

void TJsonParser::VisitArray(const TJsonValue::TArray& array)
{
    Consumer->OnBeginList();
    FOREACH (const auto& value, array) {
        Consumer->OnListItem();
        VisitAny(value);
    }
    Consumer->OnEndList();
}

void TJsonParser::VisitMap(const TJsonValue::TMap& map)
{
    auto value = map.find("$value");
    if (value != map.end()) {
        auto it = map.find("$attributes");
        if (it != map.end()) {
            const auto& attributes = it->second;
            if (attributes.GetType() != JSON_MAP) {
                ythrow yexception() << "Value of $attributes must be map";
            }
            Consumer->OnBeginAttributes();
            VisitMapItems(attributes.GetMap());
            Consumer->OnEndAttributes();
        }
        VisitAny(value->second);
    } else {
        Consumer->OnBeginMap();
        VisitMapItems(map);
        Consumer->OnEndMap();
    }
}

Stroka TJsonParser::DecodeString(const Stroka& value)
{
    if (value.empty() || value[0] != '&') return value;
    return Base64Decode(TStringBuf(value).Tail(1));
}

////////////////////////////////////////////////////////////////////////////////
            
void ParseJson(
    TInputStream* input,
    NYTree::IYsonConsumer* consumer,
    TJsonFormatConfigPtr config)
{
    TJsonParser jsonParser(consumer, config);
    jsonParser.Parse(input);
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NFormats
} // namespace NYT
