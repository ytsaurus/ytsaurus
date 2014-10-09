#pragma once

#include <core/misc/common.h>

#include <ytlib/object_client/public.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

using NObjectClient::TObjectId;
using NObjectClient::TTransactionId;
using NObjectClient::TVersionedObjectId;
using NObjectClient::EObjectType;
using NObjectClient::TCellTag;
using NObjectClient::NullObjectId;
using NObjectClient::NullTransactionId;

////////////////////////////////////////////////////////////////////////////////

struct IObjectResolver;

DECLARE_REFCOUNTED_CLASS(TObjectManager)
DECLARE_REFCOUNTED_CLASS(TGarbageCollector)

DECLARE_REFCOUNTED_CLASS(TObjectManagerConfig)
DECLARE_REFCOUNTED_CLASS(TMasterCacheServiceConfig)

class TObjectBase;
class TNonversionedObjectBase;

class TObjectProxyBase;

class TAttributeSet;

class TObjectBase;
class TNonversionedObjectBase;

class TSchemaObject;
class TMasterObject;

DECLARE_REFCOUNTED_STRUCT(IObjectProxy)
DECLARE_REFCOUNTED_STRUCT(IObjectTypeHandler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
