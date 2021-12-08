#pragma once

#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/core/misc/compact_vector.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

using NObjectClient::TObjectId;
using NObjectClient::TTransactionId;
using NObjectClient::TVersionedObjectId;
using NObjectClient::EObjectType;
using NObjectClient::TCellTag;
using NObjectClient::TCellTagList;
using NObjectClient::NullObjectId;
using NObjectClient::NullTransactionId;

////////////////////////////////////////////////////////////////////////////////

using TEpoch = ui32;

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReqCreateForeignObject;
class TReqRemoveForeignObject;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DEFINE_BIT_ENUM(ETypeFlags,
    ((None)                   (0x0000))
    ((ReplicateCreate)        (0x0001)) // replicate object creation
    ((ReplicateDestroy)       (0x0002)) // replicate object destruction
    ((ReplicateAttributes)    (0x0004)) // replicate object attribute changes
    ((Creatable)              (0x0008)) // objects of this type can be created at runtime
    ((Externalizable)         (0x0010)) // objects of this (versioned) type can be externalized to another cell (e.g. tables, files)
    ((ForbidInheritAclChange) (0x0020)) // inherit_acl attribute cannot be changed
    ((ForbidLocking)          (0x0040)) // no locks can be taken for objects of this (versioned) type
    ((TwoPhaseCreation)       (0x0080)) // employ two-phase creation protocol: CreationStarted -> CreationPreCommitted -> CreationCommitted
    ((Removable)              (0x0100)) // objects of this (unversioned) type can be removed by explicit Remove call
    ((TwoPhaseRemoval)        (0x0200)) // employ two-phase removal protocol: RemovalStarted -> RemovalPreCommitted -> RemovalComitted
    ((ForbidAnnotationRemoval)(0x0400)) // annotation cannot be removed from portal entrances and exits
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TObjectManager)
DECLARE_REFCOUNTED_CLASS(TGarbageCollector)

DECLARE_REFCOUNTED_STRUCT(TEpochContext)

DECLARE_REFCOUNTED_STRUCT(TRequestProfilingCounters)
DECLARE_REFCOUNTED_CLASS(TRequestProfilingManager)
DECLARE_REFCOUNTED_CLASS(TMutationIdempotizer)

DECLARE_REFCOUNTED_CLASS(TObjectManagerConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicObjectManagerConfig)
DECLARE_REFCOUNTED_CLASS(TObjectServiceConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicObjectServiceConfig)
DECLARE_REFCOUNTED_CLASS(TMutationIdempotizerConfig)

class TObject;

DEFINE_ENUM(EObjectPtrKind,
    (Strong)
    (Weak)
    (Ephemeral)
);

template <class T, class C>
class TObjectPtr;

struct TStrongObjectPtrContext;
template <class T>
using TStrongObjectPtr = TObjectPtr<T, TStrongObjectPtrContext>;

struct TWeakObjectPtrContext;
template <class T>
using TWeakObjectPtr = TObjectPtr<T, TWeakObjectPtrContext>;

struct TEphemeralObjectPtrContext;
template <class T>
using TEphemeralObjectPtr = TObjectPtr<T, TEphemeralObjectPtrContext>;

class TObjectProxyBase;

class TAttributeSet;

struct TObjectTypeMetadata;

DECLARE_ENTITY_TYPE(TSchemaObject, TObjectId, ::THash<TObjectId>);

class TMasterObject;

DECLARE_REFCOUNTED_STRUCT(IObjectProxy)
DECLARE_REFCOUNTED_STRUCT(IObjectTypeHandler)

DECLARE_REFCOUNTED_STRUCT(IYsonInternRegistry)

static constexpr int MaxAnnotationLength = 1024;

// NB: Changing this value requires promoting master reign.
static constexpr size_t DefaultYsonStringInternLengthThreshold = 1_KB;

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_MASTER_OBJECT_TYPE(type) \
    class type; \
    using type ## Ptr = ::NYT::NObjectServer::TStrongObjectPtr<type>; \
    \
    ::NYT::NObjectServer::TObject* ToObject(type* obj) ATTRIBUTE_USED;

#define DEFINE_MASTER_OBJECT_TYPE(type) \
    Y_FORCE_INLINE ::NYT::NObjectServer::TObject* ToObject(type* obj) \
    { \
        return obj; \
    }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
