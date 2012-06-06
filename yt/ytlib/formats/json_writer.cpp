#include "stdafx.h"
#include "json_writer.h"
#include "config.h"

#include <ytlib/ytree/null_yson_consumer.h>
#include <ytlib/misc/assert.h>

#include <util/charset/utf.h>
#include <util/string/base64.h>

namespace NYT {
namespace NFormats {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TJsonWriter::TJsonWriter(TOutputStream* output, TJsonFormatConfigPtr config)
    : UnderlyingJsonWriter(new NJson::TJsonWriter(output, false))
    , JsonWriter(~UnderlyingJsonWriter)
    , Config(config)
{
    if (!Config) {
        Config = New<TJsonFormatConfig>();
    }
}

void TJsonWriter::OnMyStringScalar(const TStringBuf& value)
{
    WriteStringScalar(value);
}

void TJsonWriter::OnMyIntegerScalar(i64 value)
{
    JsonWriter->Write(value);
}

void TJsonWriter::OnMyDoubleScalar(double value)
{
    JsonWriter->Write(value);
}

void TJsonWriter::OnMyEntity()
{
    JsonWriter->WriteNull();
}

void TJsonWriter::OnMyBeginList()
{
    JsonWriter->OpenArray();
}

void TJsonWriter::OnMyListItem()
{ }

void TJsonWriter::OnMyEndList()
{

    JsonWriter->CloseArray();
}

void TJsonWriter::OnMyBeginMap()
{
    JsonWriter->OpenMap();
}

void TJsonWriter::OnMyKeyedItem(const TStringBuf& name)
{
    WriteStringScalar(name);
}

void TJsonWriter::OnMyEndMap()
{
    JsonWriter->CloseMap();
}

void TJsonWriter::OnMyBeginAttributes()
{
    JsonWriter->OpenMap();
    JsonWriter->Write("$attributes");
    JsonWriter->OpenMap();

    ForwardedJsonWriter.Reset(new TJsonWriter(JsonWriter, Config));
    Forward(~ForwardedJsonWriter, TClosure(), EYsonType::KeyedFragment);
}

void TJsonWriter::OnMyEndAttributes()
{
    JsonWriter->CloseMap();
    JsonWriter->Write("$value");

    ForwardedJsonWriter.Reset(new TJsonWriter(JsonWriter, Config));
    Forward(~ForwardedJsonWriter,
        BIND(&TJsonWriter::OnForwardingValueFinished, Unretained(this)),
        EYsonType::Node);
}

void TJsonWriter::OnForwardingValueFinished()
{
    JsonWriter->CloseMap();
}

TJsonWriter::TJsonWriter(NJson::TJsonWriter* jsonWriter, TJsonFormatConfigPtr config)
    : JsonWriter(jsonWriter)
    , Config(config)
{ }

void TJsonWriter::WriteStringScalar(const TStringBuf &value)
{
    if (value.empty() || (value[0] != '&' && IsUtf(value))) {
        JsonWriter->Write(value);
    } else {
        JsonWriter->Write("&" + Base64Encode(value));
    }
}

void TJsonWriter::Flush()
{
    JsonWriter->Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
