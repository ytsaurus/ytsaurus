#include "stdafx.h"
#include "json_adapter.h"

#include "null_yson_consumer.h"

#include <library/json/json_writer.h>

namespace NYT {
namespace NYTree {

using NJson::TJsonWriter;

////////////////////////////////////////////////////////////////////////////////

TJsonAdapter::TJsonAdapter(TOutputStream* output)
    : JsonWriter(new TJsonWriter(output, true))
{ }

void TJsonAdapter::OnMyStringScalar(const Stroka& value, bool hasAttributes)
{
    YASSERT(!hasAttributes);
    JsonWriter->Write(value);
}

void TJsonAdapter::OnMyInt64Scalar(i64 value, bool hasAttributes)
{
    YASSERT(!hasAttributes);
    JsonWriter->Write(value);
}

void TJsonAdapter::OnMyDoubleScalar(double value, bool hasAttributes)
{
    YASSERT(!hasAttributes);
    JsonWriter->Write(value);
}

void TJsonAdapter::OnMyEntity(bool hasAttributes)
{
    UNUSED(hasAttributes);
    JsonWriter->WriteNull();
}

void TJsonAdapter::OnMyBeginList()
{
    JsonWriter->OpenArray();
}

void TJsonAdapter::OnMyListItem()
{ }

void TJsonAdapter::OnMyEndList(bool hasAttributes)
{
    JsonWriter->CloseArray();
}

void TJsonAdapter::OnMyBeginMap()
{
    JsonWriter->OpenMap();
}

void TJsonAdapter::OnMyMapItem(const Stroka& name)
{
    JsonWriter->Write(name);
}

void TJsonAdapter::OnMyEndMap(bool hasAttributes)
{
    JsonWriter->CloseMap();
}

void TJsonAdapter::OnMyBeginAttributes()
{
    ForwardAttributes(GetNullYsonConsumer(), NULL);
}

void TJsonAdapter::OnMyAttributesItem(const Stroka& name)
{
    UNUSED(name);
    YUNREACHABLE();
}

void TJsonAdapter::OnMyEndAttributes()
{
    YUNREACHABLE();
}

void TJsonAdapter::Flush()
{
    JsonWriter->Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
