#pragma once

#include <yp/server/misc/public.h>

#include <yp/server/master/public.h>

#include <yp/client/api/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/misc/guid.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TPodSpecEtc;
class TPodStatusEtc;

} // namespace NProto

DECLARE_REFCOUNTED_CLASS(TPodTypeHandlerConfig)

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

struct TDBField;
struct TDBTable;

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
class TResourceCache;
class TDynamicResource;
class TPod;
class TPodSet;
class TEndpoint;
class TEndpointSet;
class TNetworkProject;
class TReplicaSet;
class TNodeSegment;
class TDnsRecordSet;
class TVirtualService;
class TSubject;
class TUser;
class TGroup;
class TSchema;
class TInternetAddress;
class TAccount;
class TMultiClusterReplicaSet;
class TStage;
class TPodDisruptionBudget;
class TIP4Pool;

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

DEFINE_ENUM(EObjectState,
    (Unknown)
    (Instantiated)
    (Creating)
    (Created)
    (Removing)
    (Removed)
    (CreatedRemoving)
    (CreatedRemoved)
);

// Must be kept in sync with protos
DEFINE_ENUM(EObjectType,
    ((Null)                   (-1))
    ((Node)                    (0))
    ((Pod)                     (1))
    ((PodSet)                  (2))
    ((Resource)                (3))
    ((NetworkProject)          (4))
    ((Endpoint)                (5))
    ((EndpointSet)             (6))
    ((NodeSegment)             (7))
    ((VirtualService)          (8))
    ((User)                    (9))
    ((Group)                  (10))
    ((InternetAddress)        (11))
    ((Account)                (12))
    ((ReplicaSet)             (13))
    ((DnsRecordSet)           (14))
    ((ResourceCache)          (15))
    ((MultiClusterReplicaSet) (16))
    ((DynamicResource)        (17))
    // Node2 is an alias and must be processed at the Api layer without explicit declaration.
    // ((Node2)                  (18))
    ((Stage)                  (19))
    ((PodDisruptionBudget)    (20))
    ((IP4Pool)                (21))
    ((NetworkModule)         (100)) // internal, not present in data_model.proto
    ((Schema)                (256))
);

DEFINE_STRING_SERIALIZABLE_ENUM(EPodCurrentState,
    ((Unknown)         (0))
    ((StartPending)  (100))
    ((Started)       (200))
    ((StopPending)   (300))
    ((Stopped)       (400))
    ((StartFailed)   (500))
);

DEFINE_STRING_SERIALIZABLE_ENUM(EPodTargetState,
    ((Removed)         (0))
    ((Active)        (100))
);

DEFINE_STRING_SERIALIZABLE_ENUM(EResourceKind,
    ((Undefined)      (-1))
    ((Cpu)             (0))
    ((Memory)          (1))
    ((Disk)            (2))
    ((Slot)            (3))
);

DEFINE_STRING_SERIALIZABLE_ENUM(EHfsmState,
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
    ((Client)         (300))
);

DEFINE_ENUM(ESchedulingState,
    ((None)           (  0))
    ((Disabled)       (100))
    ((Pending)        (200))
    ((Assigned)       (300))
);

constexpr int TypicalDiskResourceCountPerNode = 16;
constexpr int TypicalColumnCountPerDBTable = 16;

using NClient::NApi::TObjectId;
using NClient::NApi::TTransactionId;

using NMaster::TClusterTag;
using NMaster::TMasterInstanceTag;

using NYT::NTransactionClient::TTimestamp;
using NYT::NTransactionClient::NullTimestamp;

// Built-in users.
extern const TObjectId RootUserId;

// Built-in groups.
extern const TObjectId SuperusersGroupId;

// Built-in accounts.
extern const TObjectId TmpAccountId;

//Built-in node segments.
extern const TObjectId DefaultNodeSegmentId;

// Pseudo-subjects.
extern const TObjectId EveryoneSubjectId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
