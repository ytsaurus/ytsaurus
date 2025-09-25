#pragma once

#include <yt/yt/orm/server/master/public.h>

#include <yt/yt/orm/client/objects/public.h>
#include <yt/yt/orm/client/native/public.h>

#include <yt/yt/orm/library/attributes/public.h>
#include <yt/yt/orm/library/attributes/wire_string.h>

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
//   Example: std::tuple<i64, ui64, std::string>.
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

DEFINE_STRING_SERIALIZABLE_ENUM(EEventType,
    ((None)                 (0))
    ((ObjectCreated)        (1))
    ((ObjectRemoved)        (2))
    ((ObjectUpdated)        (3))
    ((ObjectPatch)          (4))
    ((ObjectSnapshot)       (5))
    ((ObjectFinalized)      (6))
);

DEFINE_ENUM_UNKNOWN_VALUE(EEventType, None);

////////////////////////////////////////////////////////////////////////////////

struct THistoryEvent;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TGroupTypeHandlerConfig)
DECLARE_REFCOUNTED_CLASS(TObjectManager)
DECLARE_REFCOUNTED_STRUCT(TObjectManagerConfig)

DEFINE_ENUM(EYTTransactionOwnership,
    (Owned)
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
DECLARE_REFCOUNTED_STRUCT(TSelectObjectHistoryConfig)
DECLARE_REFCOUNTED_STRUCT(TBatchSizeBackoffConfig)
DECLARE_REFCOUNTED_STRUCT(THistorySnapshotPolicyConfig)
DECLARE_REFCOUNTED_STRUCT(THistoryLockerConfig)
DECLARE_REFCOUNTED_STRUCT(TTransactionManagerConfig)
DECLARE_REFCOUNTED_CLASS(TTransactionManager)
DECLARE_REFCOUNTED_CLASS(TTransaction)
DECLARE_REFCOUNTED_STRUCT(TObjectTableReaderConfig)
DECLARE_REFCOUNTED_STRUCT(TPoolWeightManagerConfig)
DECLARE_REFCOUNTED_STRUCT(IPoolWeightManager)
DECLARE_REFCOUNTED_STRUCT(IHistoryManager)

struct TReadingTransactionOptions;
struct TStartReadOnlyTransactionOptions;
using TMutatingTransactionOptions = NClient::NNative::TMutatingTransactionOptions;
using TTransactionOptions = std::variant<NClient::NNative::TMutatingTransactionOptions, TReadingTransactionOptions>;
struct TTransactionConfigsSnapshot;
struct TTransactionPerformanceStatistics;

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
    (AsObject)
);

DEFINE_ENUM(EHashPolicy,
    (NoHashColumn)
    (ParentOrObjectKey)
    (ParentKey)
    (EntireKey)
);

DECLARE_REFCOUNTED_STRUCT(TWatchLogDistributionPolicyConfig)
DECLARE_REFCOUNTED_STRUCT(TWatchManagerDistributionPolicyConfig)
DECLARE_REFCOUNTED_STRUCT(TWatchManagerConfig)
DECLARE_REFCOUNTED_CLASS(TWatchManager)

DECLARE_REFCOUNTED_STRUCT(TWatchLogChangedAttributesConfig)
DECLARE_REFCOUNTED_STRUCT(TWatchManagerChangedAttributesConfig)

DECLARE_REFCOUNTED_STRUCT(IObjectTableReader)
DECLARE_REFCOUNTED_STRUCT(IObjectTableAsyncReader)
DECLARE_REFCOUNTED_STRUCT(ISemaphoreInterop)
DECLARE_REFCOUNTED_STRUCT(IWatchLogConsumerInterop)

struct IWatchLogEventMatcher;
struct TWatchLog;

DECLARE_REFCOUNTED_CLASS(TOrderedTabletReader)

class TFetcherContext;
DECLARE_REFCOUNTED_STRUCT(IFetcher)
DECLARE_REFCOUNTED_STRUCT(ITimestampFetcher)

DECLARE_REFCOUNTED_STRUCT(IObjectFilterMatcher)

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

struct IObjectFilterMatcher;

struct IUpdateContext;

struct TTransactionCallContext;

class ISession;
struct IPersistentAttribute;
struct ILoadContext;
using TLoadCallback = std::function<void(ILoadContext*)>;
struct IStoreContext;
struct IQueryContext;

class TDBTable;
struct TObjectTable;
class TDBIndexTable;
struct TDBParentsTable;
struct TWatchLogTable;
struct TDBField;
using TDBFields = std::vector<const TDBField*>;
struct TDBFieldRef;
struct TParentsTable;

struct THistoryTimeIndexFieldsBase;

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
class TIndexAttribute;

class THistoryTimeIndexDescriptor;

// Relative load priority of attributes of the same object.
// Smaller enum value causes loading in earlier batch.
// For example, reference attributes use View priority
// because they need scalar attributes that are loaded with Default priority.
DEFINE_ENUM(ELoadPriority,
    ((Default) (0))
    ((View)    (1))
    ((Migrate) (3))
);

DEFINE_ENUM(EIndexOrReferenceMode,
    ((Disabled)          (0))
    ((RemoveOnly)        (3))
    ((Building)          (1))
    ((Enabled)           (2))
);

DEFINE_ENUM(EReferenceKind,
    (Single)
    (Multi)
    (Tabular)
    (Counting)
);

DEFINE_ENUM(EHistoryTimeIndexMode,
    ((Disabled)   (0))
    ((Enabled)    (1))
);

DEFINE_ENUM(EAttributesExtensibilityMode,
    (None)
    (Full)
    (Restricted)
);

DEFINE_ENUM(EIndexResolveStrategy,
    ((None)                        (0))
    ((ConstantConstraints)         (1))
);

DEFINE_ENUM(EInternalUpdateFormat,
    // Use `NYTree::INodePtr` for yson strings and
    // `TWireString` for protobuf payload.
    ((Optimal)       (0))
    // Convert user payload to `NYTree::INodePtr`.
    // Historic default.
    ((NodePointer)   (1))
    // Convert user payload to `TWireString`.
    // Could be used for testing and debugging.
    ((WireString)    (2))
);

struct TKeyAttributeMatches;

////////////////////////////////////////////////////////////////////////////////

struct IObjectLifecycleObserver;

class TScalarAttributeBase;

template <class T>
class TScalarAttribute;

class TTimestampAttribute;

template <class TMany, class TOne>
class TManyToOneAttribute;

template <class TOwner, class TForeign>
class TOneTransitiveAttribute;

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

struct TReferenceAttributeSettings;

struct TReferenceDescriptorBase;
class TReferenceAttributeBase;

template <class TOwner, class TForeign>
struct TSingleReferenceDescriptor;
template <class TOwner, class TForeign>
class TSingleReferenceAttribute;

template <class TOwner, class TForeign>
struct TMultiReferenceDescriptor;
template <class TOwner, class TForeign>
class TMultiReferenceAttribute;

template <class TOwner, class TForeign>
struct TTabularReferenceDescriptor;
template <class TOwner, class TForeign>
class TTabularReferenceAttribute;

template <class TOwner, class TForeign>
struct TCountingReferenceDescriptor;
template <class TOwner, class TForeign>
class TCountingReferenceAttribute;

template <class TOwner, class TForeign>
struct TSingleViewDescriptor;
template <class TOwner, class TForeign>
struct TMultiViewDescriptor;

struct TColumnarKeyStorageDescriptor;
struct TProtoKeyStorageDescriptor;
struct TObjectKeyStorageDescriptor;
struct TCompositeKeyStorageDescriptor;

using TReferenceKeyStorageDescriptor = std::variant<
    TColumnarKeyStorageDescriptor,
    TProtoKeyStorageDescriptor,
    TObjectKeyStorageDescriptor,
    TCompositeKeyStorageDescriptor>;

class TParentKeyAttribute;
class TChildrenAttributeBase;

class TAnnotationsAttribute;

template <class TTypedObject, class TTypedValue>
class TScalarAggregatedAttributeDescriptor;

template <class T>
class TScalarAggregatedAttribute;

struct TAttributeDescriptorsBase;

using TRevisionAttribute = TScalarAttribute<ui64>;

////////////////////////////////////////////////////////////////////////////////

struct IObjectTypeHandler;
class TObject;

DEFINE_ENUM(EObjectState,
    //! Object is uninitialized, it's state is unknown.
    //! It may become `Instantiated` or `Creating`.
    (Unknown)
    //! Object is requested by current transaction.
    //! Existence of the object could be determined via DidExist() and DoesExist() calls.
    (Instantiated)
    //! Object is being created. InitializeCreatedObject() has not been called.
    (Creating)
    //! Object is created. InitializeCreatedObject() has been called and attributes are initialized.
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
// TODO(dgolear): Split children finalizers?
inline constexpr TStringBuf ChildrenFinalizer = "_orm_children";
// If parent has non-finalizing children, they start finalizing until parent completes its finalization.
inline constexpr TStringBuf ParentFinalizer = "_orm_parent";

// See NProto::ECascadingRemovalPolicy for details.
DEFINE_ENUM(ECascadingRemovalPolicy,
    (Remove)
    (Finalize)
    (Forbid)
    (AwaitForeign)
    (Clear)
);

////////////////////////////////////////////////////////////////////////////////

extern const NYTree::IConstAttributeDictionaryPtr DefaultPathAttributes;

struct TResolveAttributeResult
{
    const TAttributeSchema* Attribute;
    NYPath::TYPath SuffixPath;
    NYTree::IConstAttributeDictionaryPtr PathAttributes = DefaultPathAttributes;

    bool operator==(const TResolveAttributeResult& other) const = default;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

template <>
struct THash<NYT::NOrm::NServer::NObjects::TResolveAttributeResult>
{
    size_t operator()(const NYT::NOrm::NServer::NObjects::TResolveAttributeResult& key) const;
};

namespace NYT::NOrm::NServer::NObjects {

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

struct TPerformanceStatistics;
struct TSelectStatistics;

////////////////////////////////////////////////////////////////////////////////

using NClient::NObjects::TObjectFilter;
using NClient::NObjects::TObjectId;
using NClient::NObjects::TObjectKey;
using NClient::NObjects::TObjectTypeName;
using NClient::NObjects::TObjectTypeValue;
using NClient::NObjects::TObjectTypeValues;

using NClient::NObjects::EWatchLogState;

using NClient::NObjects::EAttributeMigrationPhase;

using NClient::NObjects::ESelectObjectHistoryIndexMode;

using NClient::NObjects::NullTimestamp;
using NClient::NObjects::TTimestamp;

using NClient::NObjects::NullTransactionId;
using NClient::NObjects::TTransactionId;

using NClient::NObjects::DefaultWatchLogName;
using NClient::NObjects::EveryoneSubjectId;
using NClient::NObjects::SuperusersGroupId;
using NClient::NObjects::NullObjectId;
using NClient::NObjects::EveryoneSubjectKey;

using NClient::NObjects::TTransactionContext;

using TTimestampOrTransactionId = std::variant<TTimestamp, TTransactionId>;

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

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EUpdatePolicy,
    (Undefined)
    (Updatable)
    (OpaqueUpdatable)
    (OpaqueReadOnly)
    (ReadOnly)
    (PluginOnlyUpdatable)
);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETransactionState,
    (Active)
    (Committing)
    (Committed)
    (Failed)
    (Aborted)
    (Expired)
);

DEFINE_ENUM(ETransactionPrecommitState,
    (NotStarted)
    (Started)
    (Finished)
);

////////////////////////////////////////////////////////////////////////////////

inline constexpr int DefaultMinStringAttributeLength = 1;
inline constexpr int DefaultMaxStringAttributeLength = 256;
inline constexpr TStringBuf DefaultStringAttributeValidChars =
    "0123456789"
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "-_.:";

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

YT_DEFINE_STRONG_TYPEDEF(TObjectValidationTag, i32);

////////////////////////////////////////////////////////////////////////////////

using TTag = NClient::NObjects::TTag;
using TTagSet = NClient::NObjects::TTagSet;

////////////////////////////////////////////////////////////////////////////////

using TValueGetter = std::function<void(
    TTransaction* /*transaction*/,
    const TObject* /*object*/,
    NYson::IYsonConsumer* /*consumer*/,
    const NYPath::TYPath& /*path*/,
    const NYTree::IConstAttributeDictionaryPtr& /*pathAttributes*/)>;

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

struct TAttributeProtoVisitorContext;

// Callbacks return |true| to stop iteration (think |return found;|).
using TConstAttributeProtoVisitorCallback = std::function<bool(
    const NProtoBuf::Message* message,
    TAttributeProtoVisitorContext context)>;
using TMutableAttributeProtoVisitorCallback = std::function<bool(
    NProtoBuf::Message* message,
    TAttributeProtoVisitorContext context)>;

using NAttributes::EMissingFieldPolicy;

////////////////////////////////////////////////////////////////////////////////

inline const std::string ManagedByReferencePredicate = "ManagedByReference";

////////////////////////////////////////////////////////////////////////////////

inline const std::string DisabledIndexResolvingFlag = "PRIMARY";

////////////////////////////////////////////////////////////////////////////////

struct TGeoHashOptions;

////////////////////////////////////////////////////////////////////////////////

// See description in object.proto.
DEFINE_ENUM(EPresencePolicy,
    ((Container)        (1))
    ((NullKey)          (2))
    ((FieldPresence)    (3))
);

// How we handle repeated entries are supplied to multirefs via |Store| or by directly manipulating
// the key storage fields.
// NB: Calling |Add| with a duplicate key is always ignored; |Remove| erases all matches.
DEFINE_ENUM(EDuplicatePolicy,
    // Throw when storing duplicates. If there are duplicates in underlying storage anyway, quietly
    // deduplicate in |Load|.
    ((Deny)         (1))

    // Silently deduplicate underlying storage when storing or loading. (Loading does not modify the
    // DB, of course, but reconciling when the storage itself is modified, does.) Try to preserve
    // auxiliary data in repeated protos.
    ((Deduplicate)  (2))

    // Allow duplicates in storage; return duplicated objects.
    ((Allow)        (3))
);

DEFINE_ENUM(EOperationState,
    (Unscheduled)
    (Scheduled)
    (Running)
    (Completed)
);

DEFINE_ENUM(ECacheState,
    (Unloaded) // The cache has not been loaded yet.
    (Current) // The cache holds the current value.
    (Dirty) // The cache holds the previous value and a key field has been modified.
    (Tainted) // Somebody called MutableLoad on a key field so the cache is always invalid.
);

DEFINE_ENUM(EVersion,
    (Current) // Value as seen in this transaction.
    (Old) // Value as seen at the start of this transaction.
);

////////////////////////////////////////////////////////////////////////////////

using NAttributes::TWireStringPart;
using NAttributes::TWireString;

using TTreeView = std::variant<NYTree::INodePtr, TWireString>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define PUBLIC_INL_H_
#include "public-inl.h"
#undef PUBLIC_INL_H_
