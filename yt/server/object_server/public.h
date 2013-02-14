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
class TUnversionedObjectBase;

class TAttributeSet;

struct IObjectProxy;
typedef TIntrusivePtr<IObjectProxy> IObjectProxyPtr;

struct IObjectTypeHandler;
typedef TIntrusivePtr<IObjectTypeHandler> IObjectTypeHandlerPtr;

class TObjectService;
typedef TIntrusivePtr<TObjectService> TObjectServicePtr;

class TGarbageCollector;
typedef TIntrusivePtr<TGarbageCollector> TGarbageCollectorPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
