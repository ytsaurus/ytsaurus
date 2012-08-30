#include "stdafx.h"

#include "dsv_parser.h"
#include "yamr_base_parser.h"
#include "yamred_dsv_parser.h"

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
        NYTree::IYsonConsumer* consumer,
        TYamredDsvFormatConfigPtr config)
        : TYamrBaseParser(
              config->FieldSeparator,
              config->RecordSeparator,
              config->HasSubkey)
        , Consumer(consumer)
        , Config(config)
        , DsvParser(
            CreateParserForDsv(Consumer, Config, /*make record processing*/ false))
    { }

private:
    NYTree::IYsonConsumer* Consumer;
    TYamredDsvFormatConfigPtr Config;
    TAutoPtr<NYTree::IParser> DsvParser;

    void ConsumeFields(
        const std::vector<Stroka>& fieldNames,
        const TStringBuf& wholeField)
    {
        // Feel the power of arcadia util.
        // How elegent it cuts string using the sharp axe!
        Stroka delimiter(Config->YamrKeysSeparator);
        VectorStrok fields =
            splitStroku(
                Stroka(wholeField.begin(), wholeField.end()),
                delimiter.begin());
        if (fields.ysize() != fieldNames.size()) {
            THROW_ERROR_EXCEPTION("Invalid number of key fields: expected %d, actual %d",
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

TAutoPtr<NYTree::IParser> CreateParserForYamredDsv(
    NYTree::IYsonConsumer* consumer,
    TYamredDsvFormatConfigPtr config)
{
    if (!config) {
        config = New<TYamredDsvFormatConfig>();
    }
    return new TYamredDsvParser(consumer, config);
}

///////////////////////////////////////////////////////////////////////////////

void ParseYamredDsv(
    TInputStream* input,
    NYTree::IYsonConsumer* consumer,
    TYamredDsvFormatConfigPtr config)
{
    auto parser = CreateParserForYamredDsv(consumer, config);
    Parse(input, consumer, ~parser);
}

void ParseYamredDsv(
    const TStringBuf& data,
    NYTree::IYsonConsumer* consumer,
    TYamredDsvFormatConfigPtr config)
{
    auto parser = CreateParserForYamredDsv(consumer, config);
    parser->Read(data);
    parser->Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

