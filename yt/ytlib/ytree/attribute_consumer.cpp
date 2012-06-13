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

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NYTree
} // namespace NYT
