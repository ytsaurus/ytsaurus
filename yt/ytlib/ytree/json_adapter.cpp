#include "json_adapter.h"

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

void TJsonAdapter::StringValue(const Stroka& value)
{
    JsonWriter->Value(value);
}

void TJsonAdapter::Int64Value(i64 value)
{
    JsonWriter->Value(value);
}

void TJsonAdapter::DoubleValue(double value)
{
    JsonWriter->Value(value);
}

void TJsonAdapter::EntityValue()
{
    JsonWriter->Null();
}

void TJsonAdapter::BeginList()
{
    JsonWriter->StartArray();
}

void TJsonAdapter::ListItem(int index)
{
    //
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
    JsonWriter->Key(name);
}

void TJsonAdapter::EndMap()
{
    JsonWriter->EndObject();
}

void TJsonAdapter::BeginAttributes()
{
    assert(false);
}

void TJsonAdapter::AttributesItem(const Stroka& name)
{
    assert(false);
}

void TJsonAdapter::EndAttributes()
{
    assert(false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
