#include "json_adapter.h"

#include <library/json/json_writer.h>

namespace NYT {
namespace NYTree {

using NJson::TJsonWriter;

////////////////////////////////////////////////////////////////////////////////

TJsonAdapter::TJsonAdapter(TOutputStream* output)
    : JsonWriter(new TJsonWriter(output, true))
{ }

TJsonAdapter::~TJsonAdapter()
{
}

void TJsonAdapter::BeginTree()
{
    // ?
}

void TJsonAdapter::EndTree()
{
    // ?
}

void TJsonAdapter::StringScalar(const Stroka& value)
{
    JsonWriter->Write(value);
}

void TJsonAdapter::Int64Scalar(i64 value)
{
    JsonWriter->Write(value);
}

void TJsonAdapter::DoubleScalar(double value)
{
    JsonWriter->Write(value);
}

void TJsonAdapter::EntityScalar()
{
    // ?
}

void TJsonAdapter::BeginList()
{
    JsonWriter->OpenArray();
}

void TJsonAdapter::ListItem(int index)
{
    UNUSED(index);
}

void TJsonAdapter::EndList()
{
    JsonWriter->CloseArray();
}

void TJsonAdapter::BeginMap()
{
    JsonWriter->OpenMap();
}

void TJsonAdapter::MapItem(const Stroka& name)
{
    JsonWriter->Write(name);
}

void TJsonAdapter::EndMap()
{
    JsonWriter->CloseMap();
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
