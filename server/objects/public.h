#pragma once

#include <yp/server/misc/public.h>

#include <yp/server/master/public.h>

#include <yp/client/api/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/misc/guid.h>

namespace NYP {
namespace NServer {
namespace NObjects {

///////////////////////////////////////et_/////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TObjectManagerConfig)
DECLARE_REFCOUNTED_CLASS(TObjectManager)

DECLARE_REFCOUNTED_CLASS(TTransactionManagerConfig)
DECLARE_REFCOUNTED_CLASS(TTransactionManager)

struct IUpdateContext;
DECLARE_REFCOUNTED_CLASS(TTransaction)

struct ISession;
struct IPersistentAttribute;
struct ILoadContext;
struct IStoreContext;
struct IQueryContext;

struct TObjectFilter;

struct TDbField;
struct TDbTable;

template <class T>
class TScalarAttribute;

class TTimestampAttribute;

template <class TMany, class TOne>
class TManyToOneAttribute;

template <class TOne, class TMany>
class TOneToManyAttribute;

struct IObjectTypeHandler;
class TObject;
class TNode;
class TResource;
class TPod;
class TPodSet;
class TEndpoint;
class TEndpointSet;
class TNetworkProject;
class TNodeSegment;
class TVirtualService;

class TAttributeSchema;

template <class TTypedObject, class TTypedValue>
struct TScalarAttributeSchema;

template <class T>
class TScalarAttribute;

class TTimestampAttribute;

template <class TMany, class TOne>
struct TManyToOneAttributeSchema;

template <class TMany, class TOne>
class TManyToOneAttribute;

struct TOneToManyAttributeSchemaBase;

template <class TOne, class TMany>
struct TOneToManyAttributeSchema;

template <class TOne, class TMany>
class TOneToManyAttribute;

class TChildrenAttributeBase;

class TAnnotationsAttribute;

// Must be kept in sync with protos
DEFINE_ENUM(EObjectType,
    ((Null)         (-1))
    ((Node)          (0))
    ((Pod)           (1))
    ((PodSet)        (2))
    ((Resource)      (3))
    ((NetworkProject)(4))
    ((Endpoint)      (5))
    ((EndpointSet)   (6))
    ((NodeSegment)   (7))
    ((VirtualService)(8))
);

DEFINE_ENUM(EPodCurrentState,
    ((Unknown)         (0))
    ((StartPending)  (100))
    ((Started)       (200))
    ((StopPending)   (300))
    ((Stopped)       (400))
);

DEFINE_ENUM(EPodTargetState,
    ((Removed)         (0))
    ((Active)        (100))
);

DEFINE_ENUM(EResourceKind,
    ((Cpu)             (0))
    ((Memory)          (1))
    ((Disk)            (2))
);

DEFINE_ENUM(EHfsmState,
    ((Unknown)           (  0))
    ((Initial)           (100))
    ((Up)                (200))
    ((Down)              (300))
    ((Suspected)         (400))
    ((PrepareMaintenance)(500))
    ((Maintenance)       (600))
    ((Probation)         (700))
);

DEFINE_ENUM(ENodeMaintenanceState,
    ((None)              (  0))
    ((Requested)         (100))
    ((Acknowledged)      (200))
    ((InProgress)        (300))
);

DEFINE_ENUM(EEvictionState,
    ((None)           (  0))
    ((Requested)      (100))
    ((Acknowledged)   (200))
);

DEFINE_ENUM(EEvictionReason,
    ((None)           (  0))
    ((Hfsm)           (100))
    ((Scheduler)      (200))
);

DEFINE_ENUM(ESchedulingState,
    ((None)           (  0))
    ((Disabled)       (100))
    ((Pending)        (200))
    ((Assigned)       (300))
);

using NClient::NApi::TObjectId;
using NClient::NApi::TTransactionId;

using NMaster::TClusterTag;
using NMaster::TMasterInstanceTag;

using NYT::NTransactionClient::TTimestamp;
using NYT::NTransactionClient::NullTimestamp;

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP
