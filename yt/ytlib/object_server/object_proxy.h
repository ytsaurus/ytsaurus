#pragma once

#include "id.h"

#include <ytlib/ytree/ypath_service.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

//! Provides a way for arbitrary objects to serve YPath requests.
struct IObjectProxy
    : public virtual NYTree::IYPathService
{
    typedef TIntrusivePtr<IObjectProxy> TPtr;

    //! Returns object id.
    virtual TObjectId GetId() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

