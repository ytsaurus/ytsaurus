#pragma once

#include "id.h"
#include "object_proxy.h"

#include <ytlib/ytree/public.h>
#include <ytlib/transaction_server/public.h>
#include <ytlib/transaction_server/transaction_ypath.pb.h>
#include <ytlib/rpc/service.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

//! Provides a bridge between TObjectManager and concrete object implementations.
struct IObjectTypeHandler
    : public virtual TRefCounted
{
    //! Returns the object type handled by the handler.
    virtual EObjectType GetType() = 0;

    //! Returns true iff an object with the given id exists.
    virtual bool Exists(const TObjectId& id) = 0;

    virtual i32 RefObject(const TObjectId& id) = 0;
    virtual i32 UnrefObject(const TObjectId& id) = 0;
    virtual i32 GetObjectRefCounter(const TObjectId& id) = 0;

    //! Given a versioned object id, constructs a proxy for it.
    //! The object with the given id must exist.
    virtual IObjectProxyPtr GetProxy(
        const TObjectId& id,
        NTransactionServer::TTransaction* transaction) = 0;

    typedef NRpc::TTypedServiceRequest<NTransactionServer::NProto::TReqCreateObject> TReqCreateObject;
    typedef NRpc::TTypedServiceResponse<NTransactionServer::NProto::TRspCreateObject> TRspCreateObject;
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
    virtual TObjectId Create(
        NTransactionServer::TTransaction* transaction,
        TReqCreateObject* request,
        TRspCreateObject* response) = 0;

    //! Indicates if a valid transaction is required to create a instance.
    virtual bool IsTransactionRequired() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

