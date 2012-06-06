#pragma once

#include "public.h"
#include "config.h"

#include <ytlib/ytree/parser.h>
#include <ytlib/ytree/yson_consumer.h>

#include <library/json/json_value.h>

namespace NYT {
namespace NFormats {

// See json_writer.h for details how yson is mapped into json
// This implementation of TJsonParser is DOM-based (less effective)

////////////////////////////////////////////////////////////////////////////////

class TJsonParser
    : public NYTree::IParser
{
public:
    explicit TJsonParser(NYTree::IYsonConsumer* consumer, TJsonFormatConfigPtr config = NULL);

    virtual void Read(const TStringBuf& data);
    virtual void Finish();

    void Parse(TInputStream* input);

private:
    NYTree::IYsonConsumer* Consumer;
    TJsonFormatConfigPtr Config;

    TStringStream Stream;

    void VisitAny(const NJson::TJsonValue& value);

    void VisitMap(const NJson::TJsonValue::TMap& map);
    void VisitMapItems(const NJson::TJsonValue::TMap& map);

    void VisitArray(const NJson::TJsonValue::TArray& array);

    Stroka DecodeString(const Stroka& value);
};

////////////////////////////////////////////////////////////////////////////////

void ParseJson(
    TInputStream* input,
    NYTree::IYsonConsumer* consumer,
    TJsonFormatConfigPtr config = NULL);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
