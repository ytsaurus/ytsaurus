#pragma once

#include "attributes.h"
#include "attribute_provider.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TEphemeralAttributeProvider
    : public virtual IAttributeProvider
{
public:
    virtual IAttributeDictionary& Attributes();
    virtual const IAttributeDictionary& Attributes() const;

protected:
    bool HasAttributes() const;
    void SetAttributes(TAutoPtr<IAttributeDictionary> attributes);

private:
    TAutoPtr<IAttributeDictionary> Attributes_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
