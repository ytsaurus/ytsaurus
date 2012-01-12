#pragma once

#include "id.h"

#include <yt/ytlib/ytree/ypath_service.h>

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

    //! Returns true iff the change specified by the #context
    //! requires meta state logging.
    virtual bool IsLogged(NRpc::IServiceContext* context) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

