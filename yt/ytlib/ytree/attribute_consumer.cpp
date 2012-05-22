#include "stdafx.h"
#include "attribute_consumer.h"

#include "attributes.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TAttributeConsumer::TAttributeConsumer()
    : Attributes(CreateEphemeralAttributes())
    , Output(Value)
    , Writer(&Output)
{ }

const IAttributeDictionary& TAttributeConsumer::GetAttributes() const
{
    return *Attributes;
}

void TAttributeConsumer::OnMyKeyedItem(const TStringBuf& key)
{
    Key = key;
    ForwardNode(&Writer, BIND([=] () mutable {
        Attributes->SetYson(Key, Value);
        // TODO(babenko): "this" is needed by VC
        this->Key.clear();
        this->Value.clear();
    }));
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NYTree
} // namespace NYT
