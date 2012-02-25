#include "stdafx.h"
#include "attribute_provider_detail.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

IAttributeDictionary& TEphemeralAttributeProvider::Attributes()
{
	if (!Attributes_.Get()) {
		Attributes_ = CreateEphemeralAttributes();
	}
	return *Attributes_;
}

const IAttributeDictionary& TEphemeralAttributeProvider::Attributes() const
{
	if (Attributes_.Get()) {
		return *Attributes_;
	} else {
		return EmptyAttributes();
	}
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
