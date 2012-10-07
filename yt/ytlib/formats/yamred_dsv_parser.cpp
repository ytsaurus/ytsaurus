#include "stdafx.h"
#include "yamred_dsv_parser.h"
#include "dsv_parser.h"
#include "yamr_base_parser.h"

#include <util/string/vector.h>

namespace NYT {
namespace NFormats {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TYamredDsvParser
    : public TYamrBaseParser
{
public:
    TYamredDsvParser(
        IYsonConsumer* consumer,
        TYamredDsvFormatConfigPtr config)
        : TYamrBaseParser(
              config->FieldSeparator,
              config->RecordSeparator,
              config->HasSubkey)
        , Consumer(consumer)
        , Config(config)
        , DsvParser(CreateParserForDsv(
            Consumer,
            Config,
            /*wrapWithMap*/ false))
    { }

private:
    IYsonConsumer* Consumer;
    TYamredDsvFormatConfigPtr Config;
    TAutoPtr<IParser> DsvParser;

    void ConsumeFields(
        const std::vector<Stroka>& fieldNames,
        const TStringBuf& wholeField)
    {
        // Feel the power of arcadia util.
        // How elegent it cuts string using the sharp axe!
        Stroka delimiter(Config->YamrKeysSeparator);
        auto fields = splitStroku(
            Stroka(wholeField.begin(), wholeField.end()),
            delimiter.begin());
        
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

    void ConsumeKey(const TStringBuf& key)
    {
        Consumer->OnListItem();
        Consumer->OnBeginMap();
        ConsumeFields(Config->KeyColumnNames, key);
    }

    void ConsumeSubkey(const TStringBuf& subkey)
    {
        ConsumeFields(Config->SubkeyColumnNames, subkey);
    }

    void ConsumeValue(const TStringBuf& value)
    {
        DsvParser->Read(value);
        DsvParser->Finish();
        Consumer->OnEndMap();
    }
};

////////////////////////////////////////////////////////////////////////////////

TAutoPtr<IParser> CreateParserForYamredDsv(
    IYsonConsumer* consumer,
    TYamredDsvFormatConfigPtr config)
{
    return new TYamredDsvParser(consumer, config);
}

///////////////////////////////////////////////////////////////////////////////

void ParseYamredDsv(
    TInputStream* input,
    IYsonConsumer* consumer,
    TYamredDsvFormatConfigPtr config)
{
    auto parser = CreateParserForYamredDsv(consumer, config);
    Parse(input, consumer, ~parser);
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

