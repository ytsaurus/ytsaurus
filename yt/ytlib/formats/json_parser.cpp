#include "stdafx.h"
#include "json_parser.h"
#include "helpers.h"

#include <ytlib/misc/error.h>

#include <library/json/json_reader.h>

namespace NYT {
namespace NFormats {

using namespace NJson;

////////////////////////////////////////////////////////////////////////////////

TJsonParser::TJsonParser(
    NYson::IYsonConsumer* consumer,
    TJsonFormatConfigPtr config,
    NYson::EYsonType type)
    : Consumer(consumer)
    , Config(config)
    , Type(type)
{
    YCHECK(Type != NYson::EYsonType::MapFragment);
    if (!Config) {
        Config = New<TJsonFormatConfig>();
    }
    if (Config->Format == EJsonFormat::Pretty && Type == NYson::EYsonType::ListFragment) {
        THROW_ERROR_EXCEPTION("Pretty json format isn't supported for list fragments");
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

void TJsonParser::ParseNode(TInputStream* input)
{
    TJsonValue jsonValue;
    ReadJsonTree(input, &jsonValue);
    VisitAny(jsonValue);
}

void TJsonParser::Parse(TInputStream* input)
{
    if (Type == NYson::EYsonType::ListFragment) {
        Stroka line;
        while (input->ReadLine(line)) {
            Consumer->OnListItem();
            TStringInput stream(line);
            ParseNode(&stream);
        }
    } else {
        ParseNode(input);
    }
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
            if (IsAscii(value.GetString())) {
                Consumer->OnStringScalar(value.GetString());
            } else {
                Consumer->OnStringScalar(Utf8ToByteString(value.GetString()));
            }
            break;
        case JSON_BOOLEAN:
            THROW_ERROR_EXCEPTION("Boolean values in JSON are not supported");
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
        if (IsAscii(pair.first)) {
            Consumer->OnKeyedItem(pair.first);
        } else {
            Consumer->OnKeyedItem(Utf8ToByteString(pair.first));
        }
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
                THROW_ERROR_EXCEPTION("Value of $attributes must be map");
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

////////////////////////////////////////////////////////////////////////////////

void ParseJson(
    TInputStream* input,
    NYson::IYsonConsumer* consumer,
    TJsonFormatConfigPtr config,
    NYson::EYsonType type)
{
    TJsonParser jsonParser(consumer, config, type);
    jsonParser.Parse(input);
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NFormats
} // namespace NYT
