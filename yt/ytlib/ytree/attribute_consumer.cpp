#include "stdafx.h"
#include "attribute_consumer.h"
#include "attributes.h"

namespace NYT {
namespace NYTree {

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
    Writer.Reset(new TYsonWriter(&Output));
    Forward(Writer.Get(), BIND([=] () {
        Writer.Reset(NULL);
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

void TAttributeConsumer::OnMyIntegerScalar(i64 value)
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
    ythrow yexception() << "Attributes can only be set from a map";
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NYTree
} // namespace NYT
