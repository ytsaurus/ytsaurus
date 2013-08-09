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

class TYamredDsvConsumer
    : public TYamrConsumerBase
{
public:
    TYamredDsvConsumer(IYsonConsumer* consumer, TYamredDsvFormatConfigPtr config)
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
        // Feel the power of arcadia util.
        // How elegant it cuts string using the sharp axe!
        Stroka delimiter(Config->YamrKeysSeparator);
        Stroka wholeFieldStroka(wholeField.begin(), wholeField.end());
        VectorStrok fields = splitStroku(
            wholeFieldStroka,
            delimiter.begin(),
            /*maxFields*/ fieldNames.size());
        // Fixing bug in arcadia logic
        if (wholeFieldStroka.length() == 0) {
            fields = VectorStrok(1, "");
        }

        if (fields.size() != fieldNames.size()) {
            THROW_ERROR_EXCEPTION("Invalid number of key fields in YAMRed DSV: expected %d, actual %d",
                static_cast<int>(fieldNames.size()),
                static_cast<int>(fields.size()));
        }

        for (int i = 0; i < fields.size(); ++i) {
            Consumer->OnKeyedItem(fieldNames[i]);
            Consumer->OnStringScalar(fields[i]);
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForYamredDsv(
    IYsonConsumer* consumer,
    TYamredDsvFormatConfigPtr config)
{
    auto yamredDsvConsumer = New<TYamredDsvConsumer>(consumer, config);

    return config->Lenval
        ? std::unique_ptr<IParser>(
            new TYamrLenvalBaseParser(
                yamredDsvConsumer,
                config->HasSubkey))
        : std::unique_ptr<IParser>(
            new TYamrDelimitedBaseParser(
                yamredDsvConsumer,
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
    Parse(input, ~parser);
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

