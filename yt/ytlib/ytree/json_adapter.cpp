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

void TJsonAdapter::OnStringScalar(const Stroka& value)
{
    JsonWriter->Write(value);
}

void TJsonAdapter::OnInt64Scalar(i64 value)
{
    JsonWriter->Write(value);
}

void TJsonAdapter::OnDoubleScalar(double value)
{
    JsonWriter->Write(value);
}

void TJsonAdapter::OnEntityScalar()
{
    OnBeginMap();
    OnEndMap();
}

void TJsonAdapter::OnBeginList()
{
    JsonWriter->OpenArray();
}

void TJsonAdapter::OnListItem(int index)
{
    UNUSED(index);
}

void TJsonAdapter::OnEndList()
{
    JsonWriter->CloseArray();
}

void TJsonAdapter::OnBeginMap()
{
    JsonWriter->OpenMap();
}

void TJsonAdapter::OnMapItem(const Stroka& name)
{
    JsonWriter->Write(name);
}

void TJsonAdapter::OnEndMap()
{
    JsonWriter->CloseMap();
}

void TJsonAdapter::OnBeginAttributes()
{
    YUNIMPLEMENTED();
}

void TJsonAdapter::OnAttributesItem(const Stroka& name)
{
    UNUSED(name);
    YUNIMPLEMENTED();
}

void TJsonAdapter::OnEndAttributes()
{
    YUNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
