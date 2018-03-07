#pragma once

#include "attribute_owner.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TEphemeralAttributeOwner
    : public virtual IAttributeOwner
{
public:
    virtual const IAttributeDictionary& Attributes() const;
    virtual IAttributeDictionary* MutableAttributes();

protected:
    bool HasAttributes() const;
    void SetAttributes(std::unique_ptr<IAttributeDictionary> attributes);

private:
    std::unique_ptr<IAttributeDictionary> Attributes_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
