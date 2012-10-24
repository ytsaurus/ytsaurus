#pragma once

#include "public.h"

#include <ytlib/ytree/attribute_owner.h>
#include <ytlib/ytree/ypath_service.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

//! Provides a way for arbitrary objects to serve YPath requests.
struct IObjectProxy
    : public virtual NYTree::IYPathService
    , public virtual NYTree::IAttributeOwner
{
    //! Returns object id.
    virtual const TObjectId& GetId() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

