#include "stdafx.h"
#include "ephemeral_attribute_owner.h"
#include "attribute_helpers.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

IAttributeDictionary& TEphemeralAttributeOwner::Attributes()
{
    if (!HasAttributes()) {
        Attributes_ = CreateEphemeralAttributes();
    }
    return *Attributes_;
}

const IAttributeDictionary& TEphemeralAttributeOwner::Attributes() const
{
    if (!HasAttributes()) {
        return EmptyAttributes();
    }
    return *Attributes_;}

bool TEphemeralAttributeOwner::HasAttributes() const
{
    return Attributes_.Get();
}

void TEphemeralAttributeOwner::SetAttributes(TAutoPtr<IAttributeDictionary> attributes)
{
    Attributes_ = attributes;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
