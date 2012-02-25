#pragma once

#include "attributes.h"
#include "attribute_provider.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TEphemeralAttributeProvider
	: public virtual IAttributeDictionary
{
public:
	virtual IAttributeDictionary& Attributes();
	virtual const IAttributeDictionary& Attributes() const;

private:
	TAutoPtr<IAttributeDictionary> Attributes_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
