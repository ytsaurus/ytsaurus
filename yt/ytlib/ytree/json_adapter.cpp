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

void TJsonAdapter::OnMyStringScalar(const TStringBuf& value)
{

    JsonWriter->Write(value);
}

void TJsonAdapter::OnMyIntegerScalar(i64 value)
{

    JsonWriter->Write(value);
}

void TJsonAdapter::OnMyDoubleScalar(double value)
{

    JsonWriter->Write(value);
}

void TJsonAdapter::OnMyEntity()
{
    JsonWriter->OpenMap();
    // TODO(roizner): support attributes
    JsonWriter->Write("$type");
    JsonWriter->Write("entity");
    JsonWriter->CloseMap();
}

void TJsonAdapter::OnMyBeginList()
{
    JsonWriter->OpenArray();
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
    // TODO(roizner): support attributes
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
    // TODO(roizner): support attributes
    ForwardFragment(GetNullYsonConsumer(), TClosure());
}

//void TJsonAdapter::OnMyAttributesItem(const TStringBuf& name)
//{
//	if (WriteAttributes) {
//		// First attribute
//		WriteAttributes = false;
//		JsonWriter->Write("$attributes");
//		JsonWriter->OpenMap();
//	}
//    JsonWriter->Write(name);
//}

void TJsonAdapter::OnMyEndAttributes()
{
    //
}

void TJsonAdapter::Flush()
{
    JsonWriter->Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
