#include "json_adapter.h"

#include <util/charset/wide.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TJsonAdapter::TJsonAdapter(TJsonWriter* jsonWriter)
    : JsonWriter(jsonWriter)
{ }

void TJsonAdapter::BeginTree()
{
    JsonWriter->Start();
}

void TJsonAdapter::EndTree()
{
    JsonWriter->End();
}

void TJsonAdapter::StringScalar(const Stroka& value)
{
    JsonWriter->Value(CharToWide(value));
}

void TJsonAdapter::Int64Scalar(i64 value)
{
    JsonWriter->Value((i32) value); // temp cast
}

void TJsonAdapter::DoubleScalar(double value)
{
    JsonWriter->Value(value);
}

void TJsonAdapter::EntityScalar()
{
    JsonWriter->Null();
}

void TJsonAdapter::BeginList()
{
    JsonWriter->StartArray();
}

void TJsonAdapter::ListItem(int index)
{
    UNUSED(index);
}

void TJsonAdapter::EndList()
{
    JsonWriter->EndArray();
}

void TJsonAdapter::BeginMap()
{
    JsonWriter->StartObject();
}

void TJsonAdapter::MapItem(const Stroka& name)
{
    JsonWriter->Key(CharToWide(name));
}

void TJsonAdapter::EndMap()
{
    JsonWriter->EndObject();
}

void TJsonAdapter::BeginAttributes()
{
    YASSERT(false);
}

void TJsonAdapter::AttributesItem(const Stroka& name)
{
    UNUSED(name);
    YASSERT(false);
}

void TJsonAdapter::EndAttributes()
{
    YASSERT(false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
