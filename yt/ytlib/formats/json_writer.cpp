#include "stdafx.h"
#include "json_writer.h"
#include "config.h"

#include <ytlib/ytree/null_yson_consumer.h>
#include <ytlib/misc/assert.h>

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
    JsonWriter->Write(value);
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
    JsonWriter->OpenMap();

    JsonWriter->Write("$type");
    JsonWriter->Write("entity");

    FlushAttributes();

    JsonWriter->CloseMap();
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
    JsonWriter->Write(name);
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
