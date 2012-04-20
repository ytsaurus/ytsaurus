#include "stdafx.h"
#include "json_adapter.h"
#include "null_yson_consumer.h"

#include <ytlib/misc/assert.h>

#include <library/json/json_writer.h>

namespace NYT {
namespace NYTree {

using NJson::TJsonWriter;

////////////////////////////////////////////////////////////////////////////////

TJsonAdapter::TJsonAdapter(TOutputStream* output)
    : JsonWriter(new TJsonWriter(output, false))
    , AttributesOutput(Attributes)
    , AttributesWriter(&AttributesOutput, EYsonFormat::Binary, EYsonType::KeyedFragment)
{ }

void TJsonAdapter::OnMyStringScalar(const TStringBuf& value)
{
    JsonWriter->Write(value);
    Attributes.clear();
}

void TJsonAdapter::OnMyIntegerScalar(i64 value)
{
    JsonWriter->Write(value);
    Attributes.clear();
}

void TJsonAdapter::OnMyDoubleScalar(double value)
{
    JsonWriter->Write(value);
    Attributes.clear();
}

void TJsonAdapter::OnMyEntity()
{
    JsonWriter->OpenMap();

    // TODO(roizner): support attributes
    JsonWriter->Write("$type");
    JsonWriter->Write("entity");

    WriteAttributes();

    JsonWriter->CloseMap();
}

void TJsonAdapter::OnMyBeginList()
{
    JsonWriter->OpenArray();
    Attributes.clear();
}

void TJsonAdapter::OnMyListItem()
{ }

void TJsonAdapter::OnMyEndList()
{

    JsonWriter->CloseArray();
}

void TJsonAdapter::OnMyBeginMap()
{
    JsonWriter->OpenMap();
    WriteAttributes();
}

void TJsonAdapter::OnMyKeyedItem(const TStringBuf& name)
{
    JsonWriter->Write(name);
}

void TJsonAdapter::OnMyEndMap()
{
    JsonWriter->CloseMap();
}

void TJsonAdapter::OnMyBeginAttributes()
{
    YASSERT(Attributes.Empty());
    ForwardFragment(&AttributesWriter);
}

void TJsonAdapter::OnMyEndAttributes()
{ }

void TJsonAdapter::WriteAttributes()
{
    if (!Attributes.Empty()) {
        auto attributes = Attributes; // local copy
        Attributes.clear(); // we must clear Attributes to allow reusing this in OnRaw
        JsonWriter->Write("$attributes");
        JsonWriter->OpenMap();
        this->OnRaw(attributes, EYsonType::KeyedFragment); // it's hack
        JsonWriter->CloseMap();
    }
}

void TJsonAdapter::Flush()
{
    JsonWriter->Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
