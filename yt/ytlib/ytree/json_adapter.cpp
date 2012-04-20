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
    DiscardAttributes();
}

void TJsonAdapter::OnMyIntegerScalar(i64 value)
{
    JsonWriter->Write(value);
    DiscardAttributes();
}

void TJsonAdapter::OnMyDoubleScalar(double value)
{
    JsonWriter->Write(value);
    DiscardAttributes();
}

void TJsonAdapter::OnMyEntity()
{
    JsonWriter->OpenMap();

    JsonWriter->Write("$type");
    JsonWriter->Write("entity");

    FlushAttributes();

    JsonWriter->CloseMap();
}

void TJsonAdapter::OnMyBeginList()
{
    JsonWriter->OpenArray();
    DiscardAttributes();
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
    FlushAttributes();
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

void TJsonAdapter::FlushAttributes()
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

void TJsonAdapter::DiscardAttributes()
{
    Attributes.clear();
}

void TJsonAdapter::Flush()
{
    JsonWriter->Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
