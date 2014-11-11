#include "stdafx.h"
#include "yamred_dsv_parser.h"
#include "dsv_parser.h"
#include "yamr_base_parser.h"

#include <util/string/vector.h>

namespace NYT {
namespace NFormats {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TYamredDsvParserConsumer
    : public TYamrConsumerBase
{
public:
    TYamredDsvParserConsumer(IYsonConsumer* consumer, TYamredDsvFormatConfigPtr config)
        : TYamrConsumerBase(consumer)
        , Config(config)
        , DsvParser(CreateParserForDsv(consumer, Config, /*wrapWithMap*/ false))
    { }

    void ConsumeKey(const TStringBuf& key) override
    {
        Consumer->OnListItem();
        Consumer->OnBeginMap();
        ConsumeFields(Config->KeyColumnNames, key);
    }

    void ConsumeSubkey(const TStringBuf& subkey) override
    {
        ConsumeFields(Config->SubkeyColumnNames, subkey);
    }

    void ConsumeValue(const TStringBuf& value) override
    {
        DsvParser->Read(value);
        DsvParser->Finish();
        Consumer->OnEndMap();
    }

private:
    TYamredDsvFormatConfigPtr Config;
    std::unique_ptr<IParser> DsvParser;

    void ConsumeFields(
        const std::vector<Stroka>& fieldNames,
        const TStringBuf& wholeField)
    {
        static const char* emptyString = "";
        char delimiter = Config->YamrKeysSeparator;

        std::vector<TStringBuf> fields;
        if (wholeField.length() == 0) {
            fields = std::vector<TStringBuf>(1, TStringBuf(emptyString));
        } else {
            size_t position = 0;
            while (position != TStringBuf::npos) {
                size_t newPosition = (fields.size() + 1 == fieldNames.size())
                    ? TStringBuf::npos
                    : wholeField.find(delimiter, position);
                fields.push_back(wholeField.substr(position, newPosition));
                position = (newPosition == TStringBuf::npos) ? TStringBuf::npos : newPosition + 1;
            }
        }

        if (fields.size() != fieldNames.size()) {
            THROW_ERROR_EXCEPTION("Invalid number of key fields in YAMRed DSV: expected %v, actual %v",
                fieldNames.size(),
                fields.size());
        }

        for (int i = 0; i < fields.size(); ++i) {
            Consumer->OnKeyedItem(fieldNames[i]);
            Consumer->OnStringScalar(fields[i]);
        }
    }

};

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForYamredDsv(
    IYsonConsumer* consumer,
    TYamredDsvFormatConfigPtr config)
{
    auto parserConsumer = New<TYamredDsvParserConsumer>(consumer, config);

    return config->Lenval
        ? std::unique_ptr<IParser>(
            new TYamrLenvalBaseParser(
                parserConsumer,
                config->HasSubkey))
        : std::unique_ptr<IParser>(
            new TYamrDelimitedBaseParser(
                parserConsumer,
                config->HasSubkey,
                config->FieldSeparator,
                config->RecordSeparator,
                config->EnableEscaping, // Enable key escaping
                false, // Enable value escaping
                config->EscapingSymbol));
}

///////////////////////////////////////////////////////////////////////////////

void ParseYamredDsv(
    TInputStream* input,
    IYsonConsumer* consumer,
    TYamredDsvFormatConfigPtr config)
{
    auto parser = CreateParserForYamredDsv(consumer, config);
    Parse(input, parser.get());
}

void ParseYamredDsv(
    const TStringBuf& data,
    IYsonConsumer* consumer,
    TYamredDsvFormatConfigPtr config)
{
    auto parser = CreateParserForYamredDsv(consumer, config);
    parser->Read(data);
    parser->Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

