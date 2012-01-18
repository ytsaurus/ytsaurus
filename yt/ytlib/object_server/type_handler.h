#pragma once

#include "id.h"
#include "object_proxy.h"

#include <ytlib/ytree/ytree.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

struct IObjectTypeHandler
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IObjectTypeHandler> TPtr;

    //! Returns the object type handled by the handler.
    virtual EObjectType GetType() = 0;

    //! Returns true iff an object with the given id exists.
    virtual bool Exists(const TObjectId& id) = 0;

    virtual i32 RefObject(const TObjectId& id) = 0;
    virtual i32 UnrefObject(const TObjectId& id) = 0;
    virtual i32 GetObjectRefCounter(const TObjectId& id) = 0;

    //! Given an object id, constructs a proxy for it.
    //! The object with the given id must exist.
    virtual IObjectProxy::TPtr GetProxy(const TObjectId& id) = 0;

    //! Creates an object with the given manifest.
    /*!
     *  \returns the id of the created object.
     */
    virtual TObjectId CreateFromManifest(NYTree::IMapNode* manifest) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

