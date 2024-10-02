#pragma once

#include "public.h"

#include <yt/yt/client/table_client/row_base.h>

#include <util/generic/hash.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

extern const TDBField DummyField;
extern const TDBField FinalizationStartTimeField;
extern const TDBField FinalizersField;

////////////////////////////////////////////////////////////////////////////////

struct TDBField
{
    TString Name;
    NTableClient::EValueType Type;
    // True iff data is secure and cannot be logged.
    bool Secure = false;

    bool Evaluated = false;

    std::optional<TString> LockGroup = std::nullopt;

    bool operator==(const TDBField& other) const = default;
};

struct TDBFieldRef
{
    TString TableName;
    TString Name;
    bool Evaluated = false;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TDBField& field,
    TStringBuf /*format*/);

void FormatValue(
    TStringBuilderBase* builder,
    const TDBField* field,
    TStringBuf /*format*/);

////////////////////////////////////////////////////////////////////////////////

class TDBTable
{
public:
    explicit TDBTable(TStringBuf name);
    ~TDBTable();

    //! For convenience, field lists may contain nullptr's, which will be safely ignored.
    void Initialize(
        std::initializer_list<const TDBField*> keyFields,
        std::initializer_list<const TDBField*> otherFields);

    const TString& GetName() const;

    //! Evaluated fields (e.g.: hash) are read-only, evaluated on query by DB.
    //! Useful, for example, for building efficient filter expression (so called continuation token)
    //! to continue reading table batch-by-batch.
    //!
    //! Obviously writers must use non-evaluated fields only.
    const std::vector<const TDBField*>& GetKeyFields(bool filterEvaluatedFields) const;

    const TDBField* GetField(const TStringBuf fieldName) const;

private:
    const TString Name_;

    std::vector<const TDBField*> NonEvaluatedKeyFields_;
    std::vector<const TDBField*> KeyFields_;
    THashMap<TStringBuf, const TDBField*> NameToField_;
};

class TDBIndexTable
    : public TDBTable
{
public:
    explicit TDBIndexTable(TString name)
        : TDBTable(std::move(name))
    { }

    std::vector<const TDBField*> IndexKey;
    std::vector<const TDBField*> ObjectTableKey;
};

////////////////////////////////////////////////////////////////////////////////

extern const struct TObjectTableBase
{
    TObjectTableBase() = default;

    struct TFields
    {
        TDBField MetaCreationTime{"meta.creation_time", NTableClient::EValueType::Uint64};
        TDBField MetaRemovalTime{"meta.removal_time", NTableClient::EValueType::Uint64};
        TDBField MetaInheritAcl{"meta.inherit_acl", NTableClient::EValueType::Boolean};
        TDBField MetaAcl{"meta.acl", NTableClient::EValueType::Any};
        TDBField MetaHistorySnapshotTimestamp{"meta.history_snapshot_timestamp", NTableClient::EValueType::Uint64};
        TDBField ExistenceLock{"existence_lock", NTableClient::EValueType::Boolean};
        TDBField Labels{"labels", NTableClient::EValueType::Any};
    } Fields;
} ObjectsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TWatchLogSchema
{
    TWatchLogSchema() = default;

    const struct TKey
    {
        TDBField TabletIndex{NTableClient::TabletIndexColumnName, NTableClient::EValueType::Int64};
        TDBField RowIndex{NTableClient::RowIndexColumnName, NTableClient::EValueType::Int64};
    } Key;

    const struct TFields
    {
        TDBField Timestamp{NTableClient::TimestampColumnName, NTableClient::EValueType::Uint64};
        TDBField Record{"record", NTableClient::EValueType::Any};
    } Fields;
} WatchLogSchema;

////////////////////////////////////////////////////////////////////////////////

struct TParentsTable
    : public TDBTable
{
    explicit TParentsTable(bool addHashKeyField)
        : TDBTable("parents")
    {
        Initialize(
            /*keyFields*/ {addHashKeyField ? &Fields.Hash : nullptr, &Fields.ObjectId, &Fields.ObjectType},
            /*otherFields*/ {&Fields.ParentId});
    }

    struct TFields
    {
        TDBField Hash{.Name = "hash", .Type = NYT::NTableClient::EValueType::Uint64, .Evaluated = true};
        // Contains serialized key instead of id actually, naming is preserved for backward compatibility.
        TDBField ObjectId{"object_id", NTableClient::EValueType::String};
        TDBField ObjectType{"object_type", NTableClient::EValueType::Int64};
        // Contains serialized key instead of id actually, naming is preserved for backward compatibility.
        TDBField ParentId{"parent_id", NTableClient::EValueType::String};
    } Fields;
};

////////////////////////////////////////////////////////////////////////////////

extern const struct TTombstonesTable
    : public TDBTable
{
    TTombstonesTable()
        : TDBTable("tombstones")
    {
        Initialize(/*keyFields*/ {&Fields.ObjectId, &Fields.ObjectType}, /*otherFields*/ {&Fields.RemovalTime});
    }

    struct TFields
    {
        TDBField ObjectId{"object_id", NTableClient::EValueType::String}; // serialized key actually
        TDBField ObjectType{"object_type", NTableClient::EValueType::Int64};
        TDBField RemovalTime{"removal_time", NTableClient::EValueType::Uint64};
    } Fields;
} TombstonesTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TPendingRemovalsTable
    : public TDBTable
{
    TPendingRemovalsTable()
        : TDBTable("pending_removals")
    {
        Initialize(
            /*keyFields*/ {
                &Fields.ObjectType,
                &Fields.ObjectIdHash,
                &Fields.ObjectId
            },
            /*otherFields*/ {
                &Fields.RemovalTime
            });
    }

    struct TFields
    {
        TDBField ObjectType{"object_type", NTableClient::EValueType::Int64};
        TDBField ObjectIdHash{
            .Name = "object_id_hash",
            .Type = NTableClient::EValueType::Uint64,
            .Evaluated = true,
        };
        TDBField ObjectId{"object_id", NTableClient::EValueType::String};
        TDBField RemovalTime{"removal_time", NTableClient::EValueType::Uint64};
    } Fields;
} PendingRemovalsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TAnnotationsTable
    : public TDBTable
{
    TAnnotationsTable()
        : TDBTable("annotations")
    {
        Initialize(/*keyFields*/ {
                &Fields.ObjectId,
                &Fields.ObjectType,
                &Fields.Name
            },
            /*otherFields*/ {
                &Fields.Value
            });
    }

    struct TFields
    {
        TDBField ObjectId{"object_id", NTableClient::EValueType::String}; // serialized key actually
        TDBField ObjectType{"object_type", NTableClient::EValueType::Int64};
        TDBField Name{"name", NTableClient::EValueType::String};
        TDBField Value{"value", NTableClient::EValueType::Any};
    } Fields;
} AnnotationsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TSubjectToTypeTable
    : public TDBTable
{
    TSubjectToTypeTable()
        : TDBTable("subject_to_type")
    {
        Initialize(/*keyFields*/ {&Fields.SubjectId}, /*otherFields*/ {&Fields.Type});
    }

    struct TFields
    {
        TDBField SubjectId{"subject_id", NTableClient::EValueType::String};
        TDBField Type{"type", NTableClient::EValueType::Int64};
    } Fields;
} SubjectToTypeTable;

////////////////////////////////////////////////////////////////////////////////

struct THistoryEventsTable;

struct THistoryTableOptions
{
    THistoryTableOptions(
        bool useUuidInKey,
        bool usePositiveEventTypes,
        bool optimizeForAscendingTime,
        NObjects::EHistoryCommitTime commitTime,
        NObjects::EHistoryTimeMode timeMode)
        : UseUuidInKey(useUuidInKey)
        , UsePositiveEventTypes(usePositiveEventTypes)
        , TimeMode(timeMode)
        , OptimizeForAscendingTime(optimizeForAscendingTime)
        , CommitTime(commitTime)
    { }

    bool UseUuidInKey;
    bool UsePositiveEventTypes;
    NObjects::EHistoryTimeMode TimeMode;
    bool OptimizeForAscendingTime;
    NObjects::EHistoryCommitTime CommitTime;
};

struct THistoryTableBase
    : public THistoryTableOptions
    , public TDBTable
{
    THistoryTableBase(
        THistoryTableOptions tableOptions,
        const THistoryEventsTable* primaryTable,
        TString name)
        : THistoryTableOptions(std::move(tableOptions))
        , TDBTable(name)
        , PrimaryTable(primaryTable)
    { }

    const THistoryEventsTable* PrimaryTable;
};

////////////////////////////////////////////////////////////////////////////////

struct THistoryEventsTable
    : public THistoryTableBase
{
    THistoryEventsTable(
        bool addHashKeyField,
        bool useUuidInKey,
        bool usePositiveEventTypes,
        bool optimizeForAscendingTime,
        NObjects::EHistoryCommitTime commitTime,
        NObjects::EHistoryTimeMode timeMode,
        TString tableName)
        : THistoryTableBase(
            THistoryTableOptions(
                useUuidInKey,
                usePositiveEventTypes,
                optimizeForAscendingTime,
                commitTime,
                timeMode),
            /*primaryTable*/ this,
            tableName)
        , HasHashKeyField(addHashKeyField)
    {
        Fields.Time.Name = OptimizeForAscendingTime ? "time" : "inverted_time";
        Initialize(
            /*keyFields*/ {
                addHashKeyField ? &Fields.Hash : nullptr,
                &Fields.ObjectType,
                &Fields.ObjectId,
                useUuidInKey ? &Fields.Uuid : nullptr,
                &Fields.Time,
                &Fields.TransactionId,
                !useUuidInKey ? &Fields.EventType : nullptr,
            },
            /*otherFields*/ {
            });
    }

    struct TFields
    {
        TDBField Hash{.Name = "hash", .Type = NYT::NTableClient::EValueType::Uint64, .Evaluated = true};
        TDBField ObjectType{"object_type", NTableClient::EValueType::Int64};
        TDBField ObjectId{"object_id", NTableClient::EValueType::String}; // Serialized key actually.
        TDBField Uuid{"uuid", NTableClient::EValueType::String};
        TDBField Time{"time_uninitialized", NTableClient::EValueType::Uint64};
        TDBField TransactionId{"transaction_id", NTableClient::EValueType::String};
        TDBField EventType{"event_type", NTableClient::EValueType::Int64};
        TDBField User{"user", NTableClient::EValueType::String};
        TDBField Value{"value", NTableClient::EValueType::Any};
        TDBField HistoryEnabledAttributes{"history_enabled_attributes", NTableClient::EValueType::Any};
        TDBField Etc{"etc", NTableClient::EValueType::Any};
    } Fields;

    const bool HasHashKeyField;
};

////////////////////////////////////////////////////////////////////////////////

struct THistoryIndexTable
    : public THistoryTableBase
{
    THistoryIndexTable(
        bool addHashKeyField,
        const THistoryEventsTable& primaryTable,
        TString tableName)
        : THistoryTableBase(
            /*tableOptions*/ primaryTable,
            /*primaryTable*/ &primaryTable,
            tableName)
        , HasHashKeyField(addHashKeyField)
    {
        Fields.Time.Name = OptimizeForAscendingTime ? "time" : "inverted_time";
        Initialize(
            /*keyFields*/ {
                addHashKeyField ? &Fields.Hash : nullptr,
                &Fields.ObjectType,
                &Fields.HistoryEventType,
                &Fields.IndexEventType,
                &Fields.ObjectId,
                UseUuidInKey ? &Fields.Uuid : nullptr,
                &Fields.Time,
                &Fields.TransactionId,
            },
            /*otherFields*/ {
            });
    }

    struct TFields
    {
        TDBField Hash{.Name = "hash", .Type = NYT::NTableClient::EValueType::Uint64, .Evaluated = true};
        TDBField ObjectType{"object_type", NTableClient::EValueType::Int64};
        TDBField HistoryEventType{"history_event_type", NTableClient::EValueType::Int64};
        TDBField IndexEventType{"index_event_type", NTableClient::EValueType::String}; // Can be either index name, or attribute path.
        TDBField ObjectId{"object_id", NTableClient::EValueType::String}; // Serialized key actually.
        TDBField Uuid{"uuid", NTableClient::EValueType::String};
        TDBField Time{"time_uninitialized", NTableClient::EValueType::Uint64};
        TDBField TransactionId{"transaction_id", NTableClient::EValueType::String};
        TDBField User{"user", NTableClient::EValueType::String};
        TDBField Value{"value", NTableClient::EValueType::Any};
        TDBField Etc{"etc", NTableClient::EValueType::Any};
    } Fields;

    const bool HasHashKeyField;
};

////////////////////////////////////////////////////////////////////////////////
extern const std::vector<const TDBTable*> Tables;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
