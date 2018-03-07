#include "ephemeral_attribute_owner.h"
#include "helpers.h"

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
    return Attributes_.get();
}

bool TEphemeralAttributeOwner::HasAttributes() const
{
    return Attributes_ != nullptr;
}

void TEphemeralAttributeOwner::SetAttributes(std::unique_ptr<IAttributeDictionary> attributes)
{
    Attributes_ = std::move(attributes);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
