#pragma once

#include "public.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

struct IAttributeOwner
{
    virtual ~IAttributeOwner()
    { }

    virtual IAttributeDictionary& Attributes() = 0;
    virtual const IAttributeDictionary& Attributes() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
