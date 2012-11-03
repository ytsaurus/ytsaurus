#pragma once

#include <ytlib/misc/common.h>

#include <ytlib/object_client/public.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

using NObjectClient::TObjectId;
using NObjectClient::TTransactionId;
using NObjectClient::TVersionedObjectId;
using NObjectClient::EObjectType;
using NObjectClient::TCellId;
using NObjectClient::NullObjectId;
using NObjectClient::NullTransactionId;

////////////////////////////////////////////////////////////////////////////////

class TObjectManager;
typedef TIntrusivePtr<TObjectManager> TObjectManagerPtr;

struct TObjectManagerConfig;
typedef TIntrusivePtr<TObjectManagerConfig> TObjectManagerConfigPtr;

class TObjectBase;
class TObjectWithIdBase;

struct IObjectProxy;
typedef TIntrusivePtr<IObjectProxy> IObjectProxyPtr;

struct IObjectTypeHandler;
typedef TIntrusivePtr<IObjectTypeHandler> IObjectTypeHandlerPtr;

class TObjectService;
typedef TIntrusivePtr<TObjectService> TObjectServicePtr;

class TGarbageCollector;
typedef TIntrusivePtr<TGarbageCollector> TGarbageCollectorPtr;

////////////////////////////////////////////////////////////////////////////////
            
//! Provides means for choosing appropriate object id type from object type.
template <class T, class = void>
struct TObjectIdTraits
{ };

template <class T>
struct TObjectIdTraits<
    T,
    typename NMpl::TEnableIfC< NMpl::TIsConvertible<T, TObjectWithIdBase*>::Value >::TType>
{
    typedef TObjectId TId;
};

//! Returns the id of an object (which may be NULL).
/*!
 * This function is specialized for other object-like entities,
 * e.g. see #NYT::NChunkServer::TChunkTreeRef.
 */
template <class T>
TObjectId GetObjectId(
    T object,
    typename NMpl::TEnableIf< NMpl::TIsConvertible<T, const TObjectWithIdBase*>, void* >::TType = NULL)
{
    return object ? object->GetId() : NullObjectId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
