#include "stdafx.h"
#include "attribute_consumer.h"
#include "attributes.h"

#include <core/misc/error.h>

namespace NYT {
namespace NYTree {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TAttributeConsumer::TAttributeConsumer(IAttributeDictionary* attributes)
    : Attributes(attributes)
{ }

IAttributeDictionary* TAttributeConsumer::GetAttributes() const
{
    return Attributes;
}

void TAttributeConsumer::OnMyKeyedItem(const TStringBuf& key)
{
    Stroka localKey(key);
    Writer.reset(new TYsonWriter(&Output));
    Forward(Writer.get(), BIND([=] () {
        Writer.reset();
        Attributes->SetYson(localKey, TYsonString(Output.Str()));
        Output.clear();
    }));
}

void TAttributeConsumer::OnMyBeginMap()
{ }

void TAttributeConsumer::OnMyEndMap()
{ }

void TAttributeConsumer::OnMyBeginAttributes()
{ }

void TAttributeConsumer::OnMyEndAttributes()
{ }

void TAttributeConsumer::OnMyStringScalar(const TStringBuf& value)
{
    UNUSED(value);
    ThrowMapExpected();
}

void TAttributeConsumer::OnMyInt64Scalar(i64 value)
{
    UNUSED(value);
    ThrowMapExpected();
}

void TAttributeConsumer::OnMyDoubleScalar(double value)
{
    UNUSED(value);
    ThrowMapExpected();
}

void TAttributeConsumer::OnMyEntity()
{
    ThrowMapExpected();
}

void TAttributeConsumer::OnMyBeginList()
{
    ThrowMapExpected();
}

void TAttributeConsumer::ThrowMapExpected()
{
    THROW_ERROR_EXCEPTION("Attributes can only be set from a map");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
