#pragma once

#include <yt/yt/orm/server/master/public.h>

#include <yt/yt/orm/client/objects/public.h>
#include <yt/yt/orm/client/native/public.h>

#include <yt/yt/orm/library/query/public.h>

#include <yt/yt/client/query_client/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/misc/guid.h>

#include <yt/yt/core/yson/public.h>

#include <experimental/propagate_const>

#include <limits>
#include <variant>

namespace NYT::NOrm::NClient::NProto {

enum EAttributeSensorPolicy : int;

} // namespace NYT::NOrm::NClient::NProto

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
using TPropagateConst = std::experimental::propagate_const<T>;

////////////////////////////////////////////////////////////////////////////////

struct TDBConfig;

////////////////////////////////////////////////////////////////////////////////

struct TTestingStorageOptions
{
    bool FailSelects = false;
    bool FailLookups = false;
};

////////////////////////////////////////////////////////////////////////////////

// Separated from the object type declaration to eliminate declaration cycles.
// In particular, TManyToOneAttribute layout must not depend on TOne.
//
// TObjectKeyTraits::Types:
//   Tuple containing key field types in the key order.
//   Example: std::tuple<i64, ui64, TString>.
template <class TObject>
struct TObjectKeyTraits;

////////////////////////////////////////////////////////////////////////////////

// TObjectPluginTraits::Type:
//   TObject for objects without plugins.
//   Exact plugin type for objects with plugins.
// TObjectPluginTraits::{Up|Down}Cast:
//   Pointer conversions between plugin and base objects.
template <class TObject>
struct TObjectPluginTraits;

template <class TObject>
using TObjectPlugin = TObjectPluginTraits<TObject>::TType;

template <class TObject>
auto* DowncastObject(TObject* object);

template <class TObjectPlugin>
auto* UpcastObject(TObjectPlugin* object);

////////////////////////////////////////////////////////////////////////////////

using TEventTypeName = TString;

inline const TEventTypeName NoneEventTypeName = "none";
inline const TEventTypeName ObjectCreatedEventTypeName = "object_created";
inline const TEventTypeName ObjectRemovedEventTypeName = "object_removed";
inline const TEventTypeName ObjectUpdatedEventTypeName = "object_updated";

using TEventTypeValue = int;

inline constexpr TEventTypeValue NoneEventTypeValue = 0;
inline constexpr TEventTypeValue ObjectCreatedEventTypeValue = 1;
inline constexpr TEventTypeValue ObjectRemovedEventTypeValue = 2;
inline constexpr TEventTypeValue ObjectUpdatedEventTypeValue = 3;

////////////////////////////////////////////////////////////////////////////////

struct THistoryEvent;
using THistoryTime = std::variant<std::monostate, TInstant, NTableClient::TTimestamp>;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TGroupTypeHandlerConfig)
DECLARE_REFCOUNTED_CLASS(TObjectManager)
DECLARE_REFCOUNTED_CLASS(TObjectManagerConfig)

DEFINE_ENUM(EYTTransactionOwnership,
    (Owned)
    (NonOwnedCollocated)
    (NonOwnedAttached)
);

struct TYTTransactionDescriptor
{
    NYT::NApi::ITransactionPtr Transaction;
    EYTTransactionOwnership Ownership;
};

struct TYTClientDescriptor
{
    NYT::NApi::IClientPtr Client;
};

//! ORM read-only transaction uses client, whereas
//! read-write transaction uses YT transaction and its underlying client.
using TYTTransactionOrClientDescriptor = std::variant<
    TYTTransactionDescriptor,
    TYTClientDescriptor>;

struct THistoryTableBase;

DECLARE_REFCOUNTED_CLASS(THistoryEventCollector)
DECLARE_REFCOUNTED_CLASS(TSelectObjectHistoryConfig)
DECLARE_REFCOUNTED_CLASS(TBatchSizeBackoffConfig)
DECLARE_REFCOUNTED_CLASS(TTransactionManagerConfig)
DECLARE_REFCOUNTED_CLASS(TTransactionManager)
DECLARE_REFCOUNTED_CLASS(TTransaction)
DECLARE_REFCOUNTED_CLASS(TObjectTableReaderConfig)
DECLARE_REFCOUNTED_CLASS(TPoolWeightManagerConfig)
DECLARE_REFCOUNTED_STRUCT(IPoolWeightManager)
DECLARE_REFCOUNTED_STRUCT(IHistoryManager)

struct TMutatingTransactionOptions;
struct TReadingTransactionOptions;
using TTransactionOptions = std::variant<TMutatingTransactionOptions, TReadingTransactionOptions>;
struct TTransactionConfigsSnapshot;

DEFINE_ENUM(EHashInput,
    (ObjectKey)
    (TableKey)
    (TableKeyFirstField) // YTORM-266
);

DEFINE_ENUM(EHashMapper,
    (Range)
    (Modulo)
);

DEFINE_ENUM(EDistributionType,
    (Uniform)
    (Hash)
);

DECLARE_REFCOUNTED_CLASS(TWatchLogDistributionPolicyConfig)
DECLARE_REFCOUNTED_CLASS(TWatchManagerDistributionPolicyConfig)
DECLARE_REFCOUNTED_CLASS(TWatchManagerConfig)
DECLARE_REFCOUNTED_CLASS(TWatchManager)

DECLARE_REFCOUNTED_STRUCT(TWatchLogChangedAttributesConfig)
DECLARE_REFCOUNTED_CLASS(TWatchManagerChangedAttributesConfig)

DECLARE_REFCOUNTED_STRUCT(IObjectTableReader)
DECLARE_REFCOUNTED_STRUCT(IWatchLogConsumerInterop)

struct IWatchLogEventMatcher;
struct TWatchLog;

DECLARE_REFCOUNTED_CLASS(TOrderedTabletReader)

class TFetcherContext;
DECLARE_REFCOUNTED_STRUCT(IFetcher)
DECLARE_REFCOUNTED_STRUCT(ITimestampFetcher)

DECLARE_REFCOUNTED_STRUCT(IAttributeProfiler)

class IRelationManager;
DECLARE_REFCOUNTED_CLASS(IRelationManager)

////////////////////////////////////////////////////////////////////////////////

struct ISelectQueryExecutor;
struct IGetQueryExecutor;
struct IAggregateQueryExecutor;
struct IWatchQueryExecutor;
struct ISelectObjectHistoryExecutor;

////////////////////////////////////////////////////////////////////////////////

struct IAttributeValuesConsumer;
struct IAttributeValuesConsumerGroup;

////////////////////////////////////////////////////////////////////////////////

struct IUpdateContext;

struct TTransactionCallContext;

class ISession;
struct IPersistentAttribute;
struct ILoadContext;
struct IStoreContext;
struct IQueryContext;

class TDBTable;
class TDBIndexTable;
struct TDBField;
using TDBFields = std::vector<const TDBField*>;
struct TDBFieldRef;
struct TParentsTable;

class TAttributePermissionsCollector;

struct TObjectOrderBy;

inline constexpr int TypicalColumnCountPerDBTable = 16;

class TAttributeSchema;
class TScalarAttributeSchema;
class TCompositeAttributeSchema;
class TMetaAttributeSchema;

class TScalarAttributeSchemaBuilder;

// If `true` returned, schema traversal stops immediately and returns.
using TOnAttribute = std::function<bool(const TAttributeSchema*)>;
using TOnScalarAttribute = std::function<bool(const TScalarAttributeSchema*)>;
using TOnMutableAttribute = std::function<bool(TAttributeSchema*)>;
using TAttributeSchemaCallback = std::function<void(const TAttributeSchema*)>;

class TScalarAttributeIndexDescriptor;
struct TIndexAttribute;

// The numbers matter here: for example, parent loads must be run before default loads.
DEFINE_ENUM(ELoadPriority,
    ((Parent)  (0))
    ((Default) (1))
    ((View)    (2))
);

DEFINE_ENUM(EIndexMode,
    (Disabled)
    (Building)
    (Enabled)
);

DEFINE_ENUM(EAttributesExtensibilityMode,
    (None)
    (Full)
    (Restricted)
);

DEFINE_ENUM(EFetchRootOptimizationLevel,
    ((None)                        (0))
    ((SchemaOnly)                  (1))
    ((SquashExpressionBuilders)    (2))
    ((SquashValueGetters)          (3))
);

struct TKeyAttributeMatches;

////////////////////////////////////////////////////////////////////////////////

struct IObjectLifecycleObserver;

class TScalarAttributeBase;

template <class T>
class TScalarAttribute;

class TTimestampAttribute;

template <class TThis, class TThat>
class TOneToOneAttribute;

template <class TMany, class TOne>
class TManyToOneAttribute;

template <class TMany, class TOne>
class TTransitiveManyToOneAttribute;

class TManyToOneViewAttribute;

template <class TOne, class TMany>
class TOneToManyAttribute;

template <class TOwner, class TForeign>
class TManyToManyInlineAttribute;

class TManyToManyInlineViewAttribute;

template <class TOwner, class TForeign>
class TManyToManyTabularAttribute;

struct TScalarAttributeDescriptorBase;

template <class TTypedObject, class TTypedValue>
class TScalarAttributeDescriptor;

using TTimestampAttributeDescriptor = TScalarAttributeDescriptorBase;

template <class TThis, class TThat>
class TOneToOneAttributeDescriptor;

struct TAnyToManyAttributeDescriptorBase;

template <class TMany, class TOne>
class TManyToOneAttributeDescriptor;

template <class TMany, class TOne>
struct TManyToOneViewAttributeDescriptor;

template <class TOne, class TMany>
class TOneToManyAttributeDescriptor;

template <class TOwner, class TForeign>
class TManyToManyInlineAttributeDescriptor;

template <class TOwner, class TForeign>
struct TManyToManyInlineViewAttributeDescriptor;

template <class TOwner, class TForeign>
class TManyToManyTabularAttributeDescriptor;

template <class TOwner, class TForeign>
struct TSingleReferenceDescriptor;

template <class TOwner, class TForeign>
class TSingleReferenceAttribute;

template <class TOwner, class TForeign>
struct TMultiReferenceDescriptor;

template <class TOwner, class TForeign>
class TMultiReferenceAttribute;

template <class TOwner, class TForeign>
struct TSingleViewDescriptor;

template <class TOwner, class TForeign>
struct TMultiViewDescriptor;

struct TReferenceAttributeSettings;

struct TColumnarKeyStorageDescriptor;
class TColumnarSingleKeyStorageDriver;
class TColumnarMultiKeyStorageDriver;

struct TTabularKeyStorageDescriptor;
class TTabularMultiKeyStorageDriver;

class TParentKeyAttribute;
class TChildrenAttributeBase;

class TAnnotationsAttribute;

template <class TTypedObject, class TTypedValue>
class TScalarAggregatedAttributeDescriptor;

template <class T>
class TScalarAggregatedAttribute;

////////////////////////////////////////////////////////////////////////////////

struct IObjectTypeHandler;
class TObject;

DEFINE_ENUM(EObjectState,
    //! Object is uninitialized, it's state is unknown.
    //! It may become `Instantiated` or `Creating`.
    (Unknown)
    //! Object is requested by current transaction.
    //! Existence of the object could be determined via DidExists() and DoesExists() calls.
    (Instantiated)
    //! Object is being created. InitializeCreatedObject() has not been called.
    (Creating)
    //! Object is created. BeforeObjectsCreated() has been called and attributes are initialized.
    (Created)
    //! Object is being finalized.
    (Finalizing)
    //! Object's finalizers are finished, preparing removal. OnObjectFinalized() has already been called.
    (Finalized)
    //! Object is being removed.
    (Removing)
    //! Same as `Removing`, but for objects created within current transaction.
    (CreatedRemoving)
    //! Object is already removed. OnObjectRemoved() has already been called.
    (Removed)
    //! Same as `Removed`, but for objects created within current transaction.
    (CreatedRemoved)
);

// Parent finalization will await completion of children finalization.
inline constexpr TStringBuf ChildrenFinalizer = "_orm_children";
// If parent has non-finalizing children, they start finalizing until parent completes its finalization.
inline constexpr TStringBuf ParentFinalizer = "_orm_parent";

////////////////////////////////////////////////////////////////////////////////

struct TResolveAttributeResult
{
    const TAttributeSchema* Attribute;
    NYPath::TYPath SuffixPath;
};

////////////////////////////////////////////////////////////////////////////////

struct TAttributeValueList
{
    std::vector<NYson::TYsonString> Values;
    std::vector<NTableClient::TTimestamp> Timestamps;

    bool operator==(const TAttributeValueList& other) const = default;
};

void FormatValue(TStringBuilderBase* builder, const TAttributeValueList& valueList, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

struct TAttributeSelector
{
    std::vector<NYPath::TYPath> Paths;

    bool operator==(const TAttributeSelector& log) const = default;
};

void FormatValue(TStringBuilderBase* builder, const TAttributeSelector& selector, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

struct TPerformanceStatistics
{
    i64 ReadPhaseCount = 0;
    THashMap<TString, std::vector<NQueryClient::TQueryStatistics>> SelectQueryStatistics;

    TPerformanceStatistics& operator +=(const TPerformanceStatistics& rhs);
};

////////////////////////////////////////////////////////////////////////////////

using NClient::NObjects::TObjectFilter;
using NClient::NObjects::TObjectId;
using NClient::NObjects::TObjectKey;
using NClient::NObjects::TObjectTypeName;
using NClient::NObjects::TObjectTypeValue;
using NClient::NObjects::TObjectTypeValues;

using NClient::NObjects::EWatchLogState;

using NClient::NObjects::ESelectObjectHistoryIndexMode;

using NClient::NObjects::NullTimestamp;
using NClient::NObjects::TTimestamp;

using NClient::NObjects::NullTransactionId;
using NClient::NObjects::TTransactionId;

using NClient::NObjects::DefaultWatchLogName;

using NClient::NObjects::TTransactionContext;

using NMaster::TClusterTag;

using TTimestampOrTransactionId = std::variant<TTimestamp, TTransactionId>;

////////////////////////////////////////////////////////////////////////////////

// Builtin users.
inline const TObjectId NullObjectId = "";

// Builtin groups.
inline const TObjectId SuperusersGroupId = "superusers";

// Pseudo-subjects.
inline const TObjectId EveryoneSubjectId = "everyone";
extern const TObjectKey EveryoneSubjectKey;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESetUpdateObjectMode,
    ((Overwrite) (0))
    ((Legacy)    (1))
);

// Attribute expression context helps, for example, to determine whether it is possible
// to replace object field expression by index field expression.
DEFINE_ENUM(EAttributeExpressionContext,
    //! Attribute is referenced by a selector and will be returned after the fetch.
    (Fetch)

    //! Attribute is referenced by a filter and will restrict the set of returned objects.
    (Filter)
);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EDBVersionCompatibility,
    // Validates that master version == DB version.
    (SameAsDBVersion)
    // Validates that master version <= DB version.
    (LowerOrEqualThanDBVersion)
    // Does not validate master version against DB version.
    (DoNotValidate)
);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EHistoryCommitTime,
    (TransactionStart)
    (TransactionCommitStart)
);

DEFINE_ENUM(EHistoryTimeMode,
    (Logical)
    (Physical)
);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EUpdatePolicy,
    (Undefined)
    (Updatable)
    (OpaqueUpdatable)
    (OpaqueReadOnly)
    (ReadOnly)
);

////////////////////////////////////////////////////////////////////////////////

inline constexpr int DefaultMinStringAttributeLength = 1;
inline constexpr int DefaultMaxStringAttributeLength = 256;
inline constexpr TStringBuf DefaultStringAttributeValidChars =
    "0123456789"
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "-_.:";

inline constexpr char CompositeKeySeparator = ';';
inline constexpr char FqidSeparator = '|';

template <class TInteger>
inline constexpr TInteger DefaultMinIntegerAttributeValue = std::numeric_limits<TInteger>::min();

template <class TInteger>
inline constexpr TInteger DefaultMaxIntegerAttributeValue = std::numeric_limits<TInteger>::max();

DEFINE_STRING_SERIALIZABLE_ENUM(EAttributeGenerationPolicy,
    ((Undefined)         (0))
    ((Manual)            (1)) // Not autogenerated.
    ((Random)            (2)) // RNG fitting within validity parameters.
    ((Timestamp)         (3)) // YT timestamps, gapful, strictly monotonic, slight delay.
    ((BufferedTimestamp) (4)) // Preloaded timestamps, very fast, nonmonotonic within 1 second.
    ((IndexedIncrement)  (5)) // Lookup in index, gapless, monotonic, slow, contentious.
    ((Custom)            (6)) // Custom policy implemented in a type handler plugin.
);

////////////////////////////////////////////////////////////////////////////////

DEFINE_STRING_SERIALIZABLE_ENUM(EAttributeType,
    ((String)              (0))
    ((Int64)               (1))
    ((Uint64)              (2))
    ((Double)              (3))
    ((Boolean)             (4))
    ((Map)                 (5))
    ((List)                (6))
    ((Message)             (7))
    ((AttributeDictionary) (8))
    ((Any)                 (9))
);

////////////////////////////////////////////////////////////////////////////////

using TTag = NClient::NObjects::TTag;
using TTagSet = NClient::NObjects::TTagSet;

////////////////////////////////////////////////////////////////////////////////

using TValueGetter = std::function<void(
    TTransaction* /*transaction*/,
    const TObject* /*object*/,
    NYson::IYsonConsumer* /*consumer*/,
    const NYPath::TYPath& /*path*/)>;

////////////////////////////////////////////////////////////////////////////////

class TRequestHandler;
struct TObjectInfo;
struct TCreateObjectRequest;
struct TCreateObjectsRequest;
struct TCreateObjectResponse;
struct TCreateObjectsResponse;
struct TRemoveObjectRequest;
struct TRemoveObjectsRequest;
struct TRemoveObjectResponse;
struct TRemoveObjectsResponse;
struct TUpdateObjectRequest;
struct TUpdateObjectsSubrequest;
struct TUpdateObjectsRequest;
struct TUpdateObjectResponse;
struct TUpdateObjectsResponse;
struct TGetObjectsRequest;
struct TGetObjectsResponse;
struct TWatchObjectsRequest;
struct TWatchObjectsResponse;
struct TSelectObjectsRequest;
struct TSelectObjectsResponse;
struct TAggregateObjectsRequest;
struct TAggregateObjectsResponse;
struct TSelectObjectHistoryRequest;
struct TSelectObjectHistoryResponse;
struct TStartTransactionResponse;
struct TCommitTransactionResponse;
struct TWatchLogInfoResponse;

////////////////////////////////////////////////////////////////////////////////

using NYT::NYson::EUtf8Check;

////////////////////////////////////////////////////////////////////////////////

using EAggregateMode = NClient::NNative::EAggregateMode;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define PUBLIC_INL_H_
#include "public-inl.h"
#undef PUBLIC_INL_H_
