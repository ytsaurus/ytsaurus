#include "stdafx.h"
#include "attribute_consumer.h"

#include "attributes.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TAttributeConsumer::TAttributeConsumer(IAttributeDictionary* attributes)
    : Attributes(attributes)
    , Output(Value)
    , Writer(&Output)
{ }

IAttributeDictionary* TAttributeConsumer::GetAttributes() const
{
    return Attributes;
}

void TAttributeConsumer::OnMyKeyedItem(const TStringBuf& key)
{
    Key = key;
    Forward(&Writer, BIND([=] () {
        Attributes->SetYson(Key, Value);
        // TODO(babenko): "this" is needed by VC
        this->Key.clear();
        this->Value.clear();
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
