#include "stdafx.h"
#include "json_parser.h"
#include "helpers.h"
#include "utf8_decoder.h"

#include <core/misc/error.h>

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
    Utf8Transcoder_ = TUtf8Transcoder(Config->EncodeUtf8);
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
    TJsonReaderConfig config;
    config.MemoryLimit = Config->MemoryLimit;
    ReadJsonTree(input, &config, &jsonValue);
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
            Consumer->OnStringScalar(Utf8Transcoder_.Decode(value.GetString()));
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
        TStringBuf key = pair.first;
        const auto& value = pair.second;
        if (IsSpecialJsonKey(key)) {
            if (key.size() < 2 || key[1] != '$') {
                THROW_ERROR_EXCEPTION(
                    "Key '%s' starts with single '$'; use '$%s'"
                    "to encode this key in JSON format", ~key, ~key);
            }
            key = key.substr(1);
        }

        Consumer->OnKeyedItem(Utf8Transcoder_.Decode(key));
        VisitAny(value);
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
        if (map.find("$attributes") != map.end()) {
            THROW_ERROR_EXCEPTION("Found key `$attributes` without key `$value`");
        }
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
