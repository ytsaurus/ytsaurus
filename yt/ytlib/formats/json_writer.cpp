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
    : JsonWriter(new NJson::TJsonWriter(output, false))
    , AttributesOutput(Attributes)
    // TODO(panin): use config here
    , AttributesWriter(&AttributesOutput, EYsonFormat::Binary, EYsonType::KeyedFragment)
    , Config(config)
{
    if (!Config) {
        Config = New<TJsonFormatConfig>();
    }
}

void TJsonWriter::OnMyStringScalar(const TStringBuf& value)
{
    WriteStringScalar(value);
    DiscardAttributes();
}

void TJsonWriter::OnMyIntegerScalar(i64 value)
{
    JsonWriter->Write(value);
    DiscardAttributes();
}

void TJsonWriter::OnMyDoubleScalar(double value)
{
    JsonWriter->Write(value);
    DiscardAttributes();
}

void TJsonWriter::OnMyEntity()
{
    JsonWriter->WriteNull();
}

void TJsonWriter::OnMyBeginList()
{
    JsonWriter->OpenArray();
    DiscardAttributes();
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
    FlushAttributes();
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
    YASSERT(Attributes.Empty());
    Forward(&AttributesWriter, TClosure(), EYsonType::KeyedFragment);
}

void TJsonWriter::OnMyEndAttributes()
{ }

void TJsonWriter::WriteStringScalar(const TStringBuf &value)
{
    if (value.empty() || (value[0] != '&' && IsUtf(value))) {
        JsonWriter->Write(value);
    } else {
        JsonWriter->Write("&" + Base64Encode(value));
    }
}

void TJsonWriter::FlushAttributes()
{
    if (!Attributes.Empty()) {
        // Swap the attributes into a local variable and copy the stored copy.
        auto attributes = Attributes; // local copy
        Attributes.clear();

        JsonWriter->Write("$attributes");
        JsonWriter->OpenMap();
        OnRaw(attributes, EYsonType::KeyedFragment);
        JsonWriter->CloseMap();
    }
}

void TJsonWriter::DiscardAttributes()
{
    Attributes.clear();
}

void TJsonWriter::Flush()
{
    JsonWriter->Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
