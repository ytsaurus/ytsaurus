#pragma once

#include "id.h"
#include "object_proxy.h"

#include <ytlib/ytree/ytree.h>
#include <ytlib/transaction_server/id.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

//! Provides a bridge between TObjectManager and concrete object implementations.
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

    //! Creates a new object instance.
    /*!
     *  \param transactionId Id of the transaction that becomes the owner of the newly created object.
     *  \param manifest Manifest containing additional creation parameters. 
     *  \returns the id of the created object.
     */
    virtual TObjectId CreateFromManifest(
        const NTransactionServer::TTransactionId& transactionId,
        NYTree::IMapNode* manifest) = 0;

    //! Indicates if a valid transaction is required to create a instance.
    virtual bool IsTransactionRequired() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

