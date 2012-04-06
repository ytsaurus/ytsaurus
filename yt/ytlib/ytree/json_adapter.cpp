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
    , WriteAttributes(false)
{ }

void TJsonAdapter::OnMyStringScalar(const TStringBuf& value, bool hasAttributes)
{
    UNUSED(hasAttributes);
    JsonWriter->Write(value);
}

void TJsonAdapter::OnMyIntegerScalar(i64 value, bool hasAttributes)
{
    UNUSED(hasAttributes);
    JsonWriter->Write(value);
}

void TJsonAdapter::OnMyDoubleScalar(double value, bool hasAttributes)
{
    UNUSED(hasAttributes);
    JsonWriter->Write(value);
}

void TJsonAdapter::OnMyEntity(bool hasAttributes)
{
    JsonWriter->OpenMap();
    JsonWriter->Write("$type");
    JsonWriter->Write("entity");
    if (hasAttributes) {
        WriteAttributes = true;
    } else {
        JsonWriter->CloseMap();
    }
}

void TJsonAdapter::OnMyBeginList()
{
    JsonWriter->OpenArray();
}

void TJsonAdapter::OnMyListItem()
{ }

void TJsonAdapter::OnMyEndList(bool hasAttributes)
{
    UNUSED(hasAttributes);
    JsonWriter->CloseArray();
}

void TJsonAdapter::OnMyBeginMap()
{
    JsonWriter->OpenMap();
}

void TJsonAdapter::OnMyMapItem(const TStringBuf& name)
{
    JsonWriter->Write(name);
}

void TJsonAdapter::OnMyEndMap(bool hasAttributes)
{
    if (hasAttributes) {
        WriteAttributes = true;
    } else {
        JsonWriter->CloseMap();
    }
}

void TJsonAdapter::OnMyBeginAttributes()
{
    if (WriteAttributes) {
        WriteAttributes = false;
        JsonWriter->Write("$attributes");
        JsonWriter->OpenMap();
    } else {
        ForwardAttributes(GetNullYsonConsumer(), TClosure());
    }
}

void TJsonAdapter::OnMyAttributesItem(const TStringBuf& name)
{
    JsonWriter->Write(name);
}

void TJsonAdapter::OnMyEndAttributes()
{
    JsonWriter->CloseMap();
    JsonWriter->CloseMap();
}

void TJsonAdapter::Flush()
{
    JsonWriter->Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
