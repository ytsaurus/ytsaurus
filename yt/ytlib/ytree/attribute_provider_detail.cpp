#include "stdafx.h"
#include "attribute_provider_detail.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

IAttributeDictionary& TEphemeralAttributeProvider::Attributes()
{
    if (!HasAttributes()) {
        Attributes_ = CreateEphemeralAttributes();
    }
    return *Attributes_;
}

const IAttributeDictionary& TEphemeralAttributeProvider::Attributes() const
{
    if (!HasAttributes()) {
        return EmptyAttributes();
    }
    return *Attributes_;}

bool TEphemeralAttributeProvider::HasAttributes() const
{
    return Attributes_.Get();
}

void TEphemeralAttributeProvider::SetAttributes(TAutoPtr<IAttributeDictionary> attributes)
{
    Attributes_ = attributes;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
