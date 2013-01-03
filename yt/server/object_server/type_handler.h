#pragma once

#include "public.h"
#include "object_proxy.h"

#include <ytlib/ytree/public.h>

#include <ytlib/rpc/service.h>

#include <ytlib/transaction_client/transaction_ypath.pb.h>

#include <server/transaction_server/public.h>

#include <server/security_server/public.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EObjectTransactionMode,
    (Required)
    (Forbidden)
    (Optional)
);

DECLARE_ENUM(EObjectAccountMode,
    (Required)
    (Forbidden)
    (Optional)
);

//! Provides a bridge between TObjectManager and concrete object implementations.
struct IObjectTypeHandler
    : public virtual TRefCounted
{
    //! Returns the object type handled by the handler.
    virtual EObjectType GetType() const = 0;

    //! Finds object by id, returns |NULL| if nothing is found.
    virtual TObjectBase* FindObject(const TObjectId& id) = 0;

    //! Finds object by id, fails is nothing is found.
    TObjectBase* GetObject(const TObjectId& id);

    //! Given a versioned object id, constructs a proxy for it.
    //! The object with the given id must exist.
    virtual IObjectProxyPtr GetProxy(
        TObjectBase* object,
        NTransactionServer::TTransaction* transaction) = 0;

    typedef NRpc::TTypedServiceRequest<NTransactionClient::NProto::TReqCreateObject> TReqCreateObject;
    typedef NRpc::TTypedServiceResponse<NTransactionClient::NProto::TRspCreateObject> TRspCreateObject;
    //! Creates a new object instance.
    /*!
     *  \param transaction Transaction that becomes the owner of the newly created object.
     *  May be NULL if #IsTransactionRequired returns False.
     *  \param request Creation request (possibly containing additional parameters).
     *  \param response Creation response (which may also hold some additional result).
     *  \returns the id of the newly created object.
     *  
     *  Once #Create is completed, all request attributes are copied to object attributes.
     *  The handler may alter the request appropriately to control this process.
     */
    virtual TObjectBase* Create(
        NTransactionServer::TTransaction* transaction,
        NSecurityServer::TAccount* account,
        NYTree::IAttributeDictionary* attributes,
        TReqCreateObject* request,
        TRspCreateObject* response) = 0;

    //! Performs the necessary cleanup.
    virtual void Destroy(const TObjectId& id) = 0;

    //! Clears staging information of a given object.
    /*!
     *  If #recursive is True then also releases all child objects.
     *  
     */
    virtual void Unstage(
        TObjectBase* object,
        NTransactionServer::TTransaction* transaction,
        bool recursive) = 0;

    //! Specifies if a valid transaction is required or allowed to create a instance.
    virtual EObjectTransactionMode GetTransactionMode() const = 0;

    //! Specifies if a valid account is required or allowed to create a instance.
    virtual EObjectAccountMode GetAccountMode() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

