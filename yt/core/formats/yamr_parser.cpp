#include "stdafx.h"
#include "yamr_parser.h"
#include "yamr_base_parser.h"

#include <core/misc/error.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TYamrConsumer
    : public TYamrConsumerBase
{
public:
    TYamrConsumer(NYson::IYsonConsumer* consumer, TYamrFormatConfigPtr config)
        : TYamrConsumerBase(consumer)
        , Config(config)
    { }

    virtual void ConsumeKey(const TStringBuf& key) override
    {
        Consumer->OnListItem();
        Consumer->OnBeginMap();
        Consumer->OnKeyedItem(Config->Key);
        Consumer->OnStringScalar(key);
    }

    virtual void ConsumeSubkey(const TStringBuf& subkey) override
    {
        Consumer->OnKeyedItem(Config->Subkey);
        Consumer->OnStringScalar(subkey);
    }

    virtual void ConsumeValue(const TStringBuf& value) override
    {
        Consumer->OnKeyedItem(Config->Value);
        Consumer->OnStringScalar(value);
        Consumer->OnEndMap();
    }

private:
    TYamrFormatConfigPtr Config;
};

///////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForYamr(
    NYson::IYsonConsumer* consumer,
    TYamrFormatConfigPtr config)
{
    if (!config) {
        config = New<TYamrFormatConfig>();
    }

    auto yamrConsumer = New<TYamrConsumer>(consumer, config);

    return config->Lenval
        ? std::unique_ptr<IParser>(
            new TYamrLenvalBaseParser(
                yamrConsumer,
                config->HasSubkey))
        : std::unique_ptr<IParser>(
            new TYamrDelimitedBaseParser(
                yamrConsumer,
                config->HasSubkey,
                config->FieldSeparator,
                config->RecordSeparator,
                config->EnableEscaping, //Enable key escaping
                config->EnableEscaping, //Enable value escaping
                config->EscapingSymbol));
}

///////////////////////////////////////////////////////////////////////////////

void ParseYamr(
    TInputStream* input,
    NYson::IYsonConsumer* consumer,
    TYamrFormatConfigPtr config)
{
    auto parser = CreateParserForYamr(consumer, config);
    Parse(input, ~parser);
}

void ParseYamr(
    const TStringBuf& data,
    NYson::IYsonConsumer* consumer,
    TYamrFormatConfigPtr config)
{
    auto parser = CreateParserForYamr(consumer, config);
    parser->Read(data);
    parser->Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
