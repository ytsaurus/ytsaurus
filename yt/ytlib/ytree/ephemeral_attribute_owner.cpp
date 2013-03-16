#include "stdafx.h"
#include "ephemeral_attribute_owner.h"
#include "attribute_helpers.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

const IAttributeDictionary& TEphemeralAttributeOwner::Attributes() const
{
    if (!HasAttributes()) {
        return EmptyAttributes();
    }
    return *Attributes_;
}

IAttributeDictionary* TEphemeralAttributeOwner::MutableAttributes()
{
    if (!HasAttributes()) {
        Attributes_ = CreateEphemeralAttributes();
    }
    return Attributes_.Get();
}

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
