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

struct IObjectResolver;

class TObjectManager;
typedef TIntrusivePtr<TObjectManager> TObjectManagerPtr;

class TObjectManagerConfig;
typedef TIntrusivePtr<TObjectManagerConfig> TObjectManagerConfigPtr;

class TObjectBase;
class TNonversionedObjectBase;

class TAttributeSet;

class TObjectBase;
class TNonversionedObjectBase;

class TObjectProxyBase;

class TSchemaObject;
class TMasterObject;

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
