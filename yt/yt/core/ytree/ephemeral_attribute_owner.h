#pragma once

#include "attribute_owner.h"

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

class TEphemeralAttributeOwner
    : public virtual IAttributeOwner
{
public:
    virtual const IAttributeDictionary& Attributes() const;
    virtual IAttributeDictionary* MutableAttributes();

protected:
    bool HasAttributes() const;
    void SetAttributes(IAttributeDictionaryPtr attributes);

private:
    IAttributeDictionaryPtr Attributes_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
