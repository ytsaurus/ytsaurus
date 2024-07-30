#include "dynamic_state.h"
#include "private.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/queue_client/config.h>

#include <yt/yt/client/table_client/check_schema_compatibility.h>
#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/record_helpers.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NQueueClient {

using namespace NConcurrency;
using namespace NHiveClient;
using namespace NObjectClient;
using namespace NQueueClient;
using namespace NTableClient;
using namespace NYPath;
using namespace NApi;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = QueueClientLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

template <class T>
std::optional<TString> MapEnumToString(const std::optional<T>& optionalValue)
{
    if (optionalValue) {
        return FormatEnum(*optionalValue);
    }
    return std::nullopt;
}

template <class T>
std::optional<T> MapStringToEnum(const std::optional<TString>& optionalString)
{
    if (optionalString) {
        return ParseEnum<T>(*optionalString);
    }
    return std::nullopt;
}

template <typename T>
std::optional<TYsonString> ToOptionalYsonString(const std::optional<T>& value)
{
    if (value) {
        return ConvertToYsonString(*value);
    }
    return std::nullopt;
}

template <typename T>
std::optional<T> FromOptionalYsonString(const std::optional<TYsonString>& value)
{
    if (value) {
        return ConvertTo<T>(*value);
    }
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

TQueueTableRow RowFromRecord(const NRecords::TQueueObject& record)
{
    return TQueueTableRow{
        .Ref = TCrossClusterReference{record.Key.Cluster, record.Key.Path},
        .RowRevision = record.RowRevision,
        .Revision = record.Revision,
        .ObjectType = MapStringToEnum<EObjectType>(record.ObjectType),
        .Dynamic = record.Dynamic,
        .Sorted = record.Sorted,
        .AutoTrimConfig = FromOptionalYsonString<TQueueAutoTrimConfig>(record.AutoTrimConfig).value_or(TQueueAutoTrimConfig()),
        .StaticExportConfig = FromOptionalYsonString<THashMap<TString, TQueueStaticExportConfig>>(record.StaticExportConfig),
        .QueueAgentStage = record.QueueAgentStage,
        .ObjectId = record.ObjectId,
        .QueueAgentBanned = record.QueueAgentBanned,
        .SynchronizationError = FromOptionalYsonString<TError>(record.SynchronizationError),
    };
}

NRecords::TQueueObjectKey RecordKeyFromRow(const TQueueTableRow& row)
{
    return NRecords::TQueueObjectKey{
        .Cluster = row.Ref.Cluster,
        .Path = row.Ref.Path,
    };
}

NRecords::TQueueObject RecordFromRow(const TQueueTableRow& row)
{
    return NRecords::TQueueObject{
        .Key = RecordKeyFromRow(row),
        .RowRevision = row.RowRevision,
        .Revision = row.Revision,
        .ObjectType = MapEnumToString(row.ObjectType),
        .Dynamic = row.Dynamic,
        .Sorted = row.Sorted,
        .AutoTrimConfig = ConvertToYsonString(row.AutoTrimConfig),
        .StaticExportConfig = ToOptionalYsonString(row.StaticExportConfig),
        .QueueAgentStage = row.QueueAgentStage,
        .ObjectId = row.ObjectId,
        .SynchronizationError = ToOptionalYsonString(row.SynchronizationError),
        .QueueAgentBanned = row.QueueAgentBanned,
    };
}

TConsumerTableRow RowFromRecord(const NRecords::TConsumerObject& record)
{
    std::optional<TTableSchema> schema;
    if (record.Schema) {
        auto workaroundVector = ConvertTo<std::vector<TTableSchema>>(*record.Schema);
        YT_VERIFY(workaroundVector.size() == 1);
        schema = workaroundVector.back();
    }

    return TConsumerTableRow{
        .Ref = TCrossClusterReference{record.Key.Cluster, record.Key.Path},
        .RowRevision = record.RowRevision,
        .Revision = record.Revision,
        .ObjectType = MapStringToEnum<EObjectType>(record.ObjectType),
        .TreatAsQueueConsumer = record.TreatAsQueueConsumer,
        .Schema = std::move(schema),
        .QueueAgentStage = record.QueueAgentStage,
        .SynchronizationError = FromOptionalYsonString<TError>(record.SynchronizationError),
    };
}

NRecords::TConsumerObjectKey RecordKeyFromRow(const TConsumerTableRow& row)
{
    return NRecords::TConsumerObjectKey{
        .Cluster = row.Ref.Cluster,
        .Path = row.Ref.Path,
    };
}
NRecords::TConsumerObject RecordFromRow(const TConsumerTableRow& row)
{
    std::optional<TYsonString> schema;
    if (row.Schema) {
        // NB(max42): Enclosing into a list is a workaround for storing YSON with top-level attributes.
        schema = ConvertToYsonString(std::array{*row.Schema});
    }

    return NRecords::TConsumerObject{
        .Key = RecordKeyFromRow(row),
        .RowRevision = row.RowRevision,
        .Revision = row.Revision,
        .ObjectType = MapEnumToString(row.ObjectType),
        .TreatAsQueueConsumer = row.TreatAsQueueConsumer,
        .Schema = schema,
        .QueueAgentStage = row.QueueAgentStage,
        .SynchronizationError = ToOptionalYsonString(row.SynchronizationError),
    };
}

TConsumerRegistrationTableRow RowFromRecord(const NRecords::TConsumerRegistration& record)
{
    const auto& key = record.Key;
    return TConsumerRegistrationTableRow{
        .Queue = TCrossClusterReference{key.QueueCluster, key.QueuePath},
        .Consumer = TCrossClusterReference{key.ConsumerCluster, key.ConsumerPath},
        .Vital = record.Vital.value_or(false),
        .Partitions = FromOptionalYsonString<std::vector<int>>(record.Partitions),
    };
}

NRecords::TConsumerRegistrationKey RecordKeyFromRow(const TConsumerRegistrationTableRow& row)
{
    return NRecords::TConsumerRegistrationKey{
        .QueueCluster = row.Queue.Cluster,
        .QueuePath = row.Queue.Path,
        .ConsumerCluster = row.Consumer.Cluster,
        .ConsumerPath = row.Consumer.Path,
    };
}

NRecords::TConsumerRegistration RecordFromRow(const TConsumerRegistrationTableRow& row)
{
    return NRecords::TConsumerRegistration{
        .Key = RecordKeyFromRow(row),
        .Vital = row.Vital,
        .Partitions = ToOptionalYsonString(row.Partitions),
    };
}

TQueueAgentObjectMappingTableRow RowFromRecord(const NRecords::TQueueAgentObjectMapping& record)
{
    return TQueueAgentObjectMappingTableRow{
        .Object = TCrossClusterReference::FromString(record.Key.Object),
        .QueueAgentHost = record.QueueAgentHost,
    };
}

NRecords::TQueueAgentObjectMappingKey RecordKeyFromRow(const TQueueAgentObjectMappingTableRow& row)
{
    return NRecords::TQueueAgentObjectMappingKey{
        .Object = ToString(row.Object),
    };
}

NRecords::TQueueAgentObjectMapping RecordFromRow(const TQueueAgentObjectMappingTableRow& row)
{
    return NRecords::TQueueAgentObjectMapping{
        .Key = RecordKeyFromRow(row),
        .QueueAgentHost = row.QueueAgentHost,
    };
}

TReplicatedTableMappingTableRow RowFromRecord(const NRecords::TReplicatedTableMapping& record)
{
    return TReplicatedTableMappingTableRow{
        .Ref = TCrossClusterReference{record.Key.Cluster, record.Key.Path},
        .Revision = record.Revision,
        .ObjectType = MapStringToEnum<EObjectType>(record.ObjectType),
        .Meta = FromOptionalYsonString<TGenericReplicatedTableMetaPtr>(record.Meta).value_or(nullptr),
        .SynchronizationError = FromOptionalYsonString<TError>(record.SynchronizationError),
    };
}

NRecords::TReplicatedTableMappingKey RecordKeyFromRow(const TReplicatedTableMappingTableRow& row)
{
    return NRecords::TReplicatedTableMappingKey{
        .Cluster = row.Ref.Cluster,
        .Path = row.Ref.Path,
    };
}

NRecords::TReplicatedTableMapping RecordFromRow(const TReplicatedTableMappingTableRow& row)
{
    std::optional<TYsonString> meta;
    if (row.Meta) {
        meta = ConvertToYsonString(row.Meta);
    }

    return NRecords::TReplicatedTableMapping{
        .Key = RecordKeyFromRow(row),
        .Revision = row.Revision,
        .ObjectType = MapEnumToString(row.ObjectType),
        .Meta = std::move(meta),
        .SynchronizationError = ToOptionalYsonString(row.SynchronizationError),
    };
}

////////////////////////////////////////////////////////////////////////////////

//! Returns remote client from client directory for the given cluster.
//! Falls back to the given local client if cluster is null or the corresponding client is not found.
IClientPtr GetRemoteClient(
    const IClientPtr& localClient,
    const TClientDirectoryPtr& clientDirectory,
    const std::optional<TString>& cluster)
{
    if (cluster) {
        if (auto remoteClient = clientDirectory->FindClient(*cluster)) {
            return remoteClient;
        }
    }

    return localClient;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

template <typename TRow, typename TRecordDescriptor>
TTableBase<TRow, TRecordDescriptor>::TTableBase(TYPath path, NApi::IClientPtr client)
    : Path_(std::move(path))
    , Client_(std::move(client))
{ }

template <typename TRow, typename TRecordDescriptor>
TFuture<std::vector<TRow>> TTableBase<TRow, TRecordDescriptor>::Select(TStringBuf where) const
{
    TString query = Format("* from [%v] where %v", Path_, where);

    YT_LOG_DEBUG(
        "Invoking select query (Query: %v)",
        query);

    return Client_->SelectRows(query)
        .Apply(BIND([&] (const TSelectRowsResult& result) {
            std::vector<TRecord> records = ToRecords<TRecord>(result.Rowset);

            std::vector<TRow> rows;
            rows.reserve(records.size());
            for (const auto& record : records) {
                rows.push_back(RowFromRecord(record));
            }

            return rows;
        }));
}

template <typename TRow, typename TRecordDescriptor>
TFuture<TTransactionCommitResult> TTableBase<TRow, TRecordDescriptor>::Insert(TRange<TRow> rows) const
{
    std::vector<TRecord> records;
    records.reserve(rows.size());
    for (const auto& row : rows) {
        records.push_back(RecordFromRow(row));
    }

    return Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet)
        .Apply(BIND([records = std::move(records), path = Path_] (const ITransactionPtr& transaction) {
            auto recordsRange = FromRecords(TRange(records));

            transaction->WriteRows(path, TRecordDescriptor::Get()->GetNameTable(), recordsRange, {.RequireSyncReplica = false});
            return transaction->Commit();
        }));
}

template <typename TRow, typename TRecordDescriptor>
TFuture<TTransactionCommitResult> TTableBase<TRow, TRecordDescriptor>::Delete(TRange<TRow> keys) const
{
    std::vector<TRecordKey> recordKeys;
    recordKeys.reserve(keys.size());
    for (const auto& key : keys) {
        recordKeys.push_back(RecordKeyFromRow(key));
    }

    return Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet)
        .Apply(BIND([recordKeys = std::move(recordKeys), path = Path_] (const ITransactionPtr& transaction) {
            auto recordKeysRange = FromRecordKeys(TRange(recordKeys));
            transaction->DeleteRows(path, TRecordDescriptor::Get()->GetNameTable(), recordKeysRange, {.RequireSyncReplica = false});
            return transaction->Commit();
        }));
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TString> TQueueTableRow::GetCypressAttributeNames()
{
    return {
        "attribute_revision",
        "type",
        "dynamic",
        "sorted",
        "auto_trim_config",
        "static_export_config",
        "queue_agent_stage",
        "id",
        "queue_agent_banned",
        // Replicated tables and chaos replicated tables.
        "replicas",
        // Chaos replicated tables.
        "replication_card_id",
        "treat_as_queue_consumer"
    };
}

TQueueTableRow TQueueTableRow::FromAttributeDictionary(
    const TCrossClusterReference& queue,
    std::optional<TRowRevision> rowRevision,
    const IAttributeDictionaryPtr& cypressAttributes)
{
    return {
        .Ref = queue,
        .RowRevision = rowRevision,
        .Revision = cypressAttributes->Find<NHydra::TRevision>("attribute_revision"),
        .ObjectType = cypressAttributes->Find<EObjectType>("type"),
        .Dynamic = cypressAttributes->Find<bool>("dynamic"),
        .Sorted = cypressAttributes->Find<bool>("sorted"),
        .AutoTrimConfig = cypressAttributes->Find<TQueueAutoTrimConfig>("auto_trim_config").value_or(TQueueAutoTrimConfig()),
        .StaticExportConfig = cypressAttributes->Find<THashMap<TString, TQueueStaticExportConfig>>("static_export_config"),
        .QueueAgentStage = cypressAttributes->Find<TString>("queue_agent_stage"),
        .ObjectId = cypressAttributes->Find<TObjectId>("id"),
        .QueueAgentBanned = cypressAttributes->Find<bool>("queue_agent_banned"),
        .SynchronizationError = TError(),
    };
}

void Serialize(const TQueueTableRow& row, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("queue").Value(row.Ref)
            .Item("row_revision").Value(row.RowRevision)
            .Item("revision").Value(row.Revision)
            .Item("object_type").Value(row.ObjectType)
            .Item("dynamic").Value(row.Dynamic)
            .Item("sorted").Value(row.Sorted)
            .Item("auto_trim_config").Value(row.AutoTrimConfig)
            .Item("static_export_config").Value(row.StaticExportConfig)
            .Item("queue_agent_stage").Value(row.QueueAgentStage)
            .Item("object_id").Value(row.ObjectId)
            .Item("queue_agent_banned").Value(row.QueueAgentBanned)
            .Item("synchronization_error").Value(row.SynchronizationError)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

template class TTableBase<TQueueTableRow, NRecords::TQueueObjectDescriptor>;

TQueueTable::TQueueTable(TYPath root, IClientPtr client)
    : TTableBase<TQueueTableRow, NRecords::TQueueObjectDescriptor>(root + "/" + NRecords::TQueueObjectDescriptor::Name, std::move(client))
{ }

////////////////////////////////////////////////////////////////////////////////

std::vector<TString> TConsumerTableRow::GetCypressAttributeNames()
{
    return {
        "attribute_revision",
        "type",
        "treat_as_queue_consumer",
        "schema",
        "queue_agent_stage",
        // Replicated tables and chaos replicated tables.
        "replicas",
        // Chaos replicated tables.
        "replication_card_id"
    };
}

TConsumerTableRow TConsumerTableRow::FromAttributeDictionary(
    const TCrossClusterReference& consumer,
    std::optional<TRowRevision> rowRevision,
    const IAttributeDictionaryPtr& cypressAttributes)
{
    return {
        .Ref = consumer,
        .RowRevision = rowRevision,
        .Revision = cypressAttributes->Get<NHydra::TRevision>("attribute_revision"),
        .ObjectType = cypressAttributes->Get<EObjectType>("type"),
        .TreatAsQueueConsumer = cypressAttributes->Get<bool>("treat_as_queue_consumer", false),
        .Schema = cypressAttributes->Find<TTableSchema>("schema"),
        .QueueAgentStage = cypressAttributes->Find<TString>("queue_agent_stage"),
        .SynchronizationError = TError(),
    };
}

void Serialize(const TConsumerTableRow& row, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("consumer").Value(row.Ref)
            .Item("row_revision").Value(row.RowRevision)
            .Item("revision").Value(row.Revision)
            .Item("object_type").Value(row.ObjectType)
            .Item("treat_as_queue_consumer").Value(row.TreatAsQueueConsumer)
            .Item("schema").Value(row.Schema)
            .Item("queue_agent_stage").Value(row.QueueAgentStage)
            .Item("synchronization_error").Value(row.SynchronizationError)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

template class TTableBase<TConsumerTableRow, NRecords::TConsumerObjectDescriptor>;

TConsumerTable::TConsumerTable(TYPath root, IClientPtr client)
    : TTableBase<TConsumerTableRow, NRecords::TConsumerObjectDescriptor>(root + "/" + NRecords::TConsumerObjectDescriptor::Name, std::move(client))
{ }

////////////////////////////////////////////////////////////////////////////////

THashMap<TCrossClusterReference, TString> TQueueAgentObjectMappingTable::ToMapping(
    const std::vector<TQueueAgentObjectMappingTableRow>& rows)
{
    THashMap<TCrossClusterReference, TString> objectMapping;
    for (const auto& row : rows) {
        objectMapping[row.Object] = row.QueueAgentHost;
    }
    return objectMapping;
}

////////////////////////////////////////////////////////////////////////////////

template class TTableBase<TQueueAgentObjectMappingTableRow, NRecords::TQueueAgentObjectMappingDescriptor>;

TQueueAgentObjectMappingTable::TQueueAgentObjectMappingTable(TYPath root, IClientPtr client)
    : TTableBase<TQueueAgentObjectMappingTableRow, NRecords::TQueueAgentObjectMappingDescriptor>(root + "/" + NRecords::TQueueAgentObjectMappingDescriptor::Name, std::move(client))
{ }

////////////////////////////////////////////////////////////////////////////////

template class TTableBase<TConsumerRegistrationTableRow, NRecords::TConsumerRegistrationDescriptor>;

TConsumerRegistrationTable::TConsumerRegistrationTable(TYPath path, IClientPtr client)
    : TTableBase<TConsumerRegistrationTableRow, NRecords::TConsumerRegistrationDescriptor>(std::move(path), std::move(client))
{ }

////////////////////////////////////////////////////////////////////////////////

void TReplicaInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("cluster_name", &TThis::ClusterName);
    registrar.Parameter("replica_path", &TThis::ReplicaPath);
    registrar.Parameter("state", &TThis::State);
    registrar.Parameter("mode", &TThis::Mode);
}

void TChaosReplicaInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("content_type", &TThis::ContentType);
}

void TReplicatedTableMeta::Register(TRegistrar registrar)
{
    registrar.Parameter("replicas", &TThis::Replicas)
        .Default();
}

void TChaosReplicatedTableMeta::Register(TRegistrar registrar)
{
    registrar.Parameter("replication_card_id", &TThis::ReplicationCardId)
        .Default(NullObjectId);
    registrar.Parameter("replicas", &TThis::Replicas)
        .Default();
}

void TGenericReplicatedTableMeta::Register(TRegistrar registrar)
{
    registrar.Parameter("replicated_table_meta", &TThis::ReplicatedTableMeta)
        .Default();
    registrar.Parameter("chaos_replicated_table_meta", &TThis::ChaosReplicatedTableMeta)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TGenericReplicatedTableMetaPtr ParseReplicatedTableMeta(EObjectType objectType, const IAttributeDictionaryPtr& cypressAttributes)
{
    auto genericReplicatedTableMeta = New<TGenericReplicatedTableMeta>();

    auto attributesAsMapNode = cypressAttributes->ToMap();
    switch (objectType) {
        case EObjectType::ReplicatedTable:
            genericReplicatedTableMeta->ReplicatedTableMeta = ConvertTo<TReplicatedTableMetaPtr>(attributesAsMapNode);
            break;
        case EObjectType::ChaosReplicatedTable:
            genericReplicatedTableMeta->ChaosReplicatedTableMeta = ConvertTo<TChaosReplicatedTableMetaPtr>(attributesAsMapNode);
            break;
        default:
            break;
    }

    return genericReplicatedTableMeta;
}

TReplicatedTableMappingTableRow TReplicatedTableMappingTableRow::FromAttributeDictionary(
    const TCrossClusterReference& object,
    const IAttributeDictionaryPtr& cypressAttributes)
{
    auto objectType = cypressAttributes->Get<EObjectType>("type");
    return {
        .Ref = object,
        .Revision = cypressAttributes->Get<NHydra::TRevision>("attribute_revision"),
        .ObjectType = objectType,
        .Meta = ParseReplicatedTableMeta(objectType, cypressAttributes),
        .SynchronizationError = TError(),
    };
}

std::vector<TRichYPath> TReplicatedTableMappingTableRow::GetReplicas(
    std::optional<NTabletClient::ETableReplicaMode> mode,
    std::optional<NTabletClient::ETableReplicaContentType> contentType) const
{
    std::vector<TRichYPath> replicas;

    if (ObjectType && *ObjectType == EObjectType::ReplicatedTable && Meta && Meta->ReplicatedTableMeta) {
        for (const auto& replica : GetValues(Meta->ReplicatedTableMeta->Replicas)) {
            if (!mode || *mode == replica->Mode) {
                replicas.push_back(TCrossClusterReference{replica->ClusterName, replica->ReplicaPath});
            }
        }
    }

    if (ObjectType && *ObjectType == EObjectType::ChaosReplicatedTable && Meta && Meta->ChaosReplicatedTableMeta) {
        for (const auto& replica : GetValues(Meta->ChaosReplicatedTableMeta->Replicas)) {
            if ((!mode || *mode == replica->Mode) && (!contentType || *contentType == replica->ContentType)) {
                replicas.push_back(TCrossClusterReference{replica->ClusterName, replica->ReplicaPath});
            }
        }
    }

    return replicas;
}

void TReplicatedTableMappingTableRow::Validate() const
{
    if (!ObjectType) {
        THROW_ERROR_EXCEPTION("Invalid replicated table mapping row for object %Qv: object type cannot be null", Ref);
    }

    if (!Meta) {
        THROW_ERROR_EXCEPTION(
            "Invalid replicated table mapping row for object %Qv of type %Qlv: meta cannot be null",
            Ref,
            *ObjectType);
    }

    switch (*ObjectType) {
        case EObjectType::ReplicatedTable:
            THROW_ERROR_EXCEPTION_IF(
                !Meta->ReplicatedTableMeta,
                "Invalid replicated table mapping row for replicated table %Qv: replicated table meta cannot be null",
                Ref);
            break;
        case EObjectType::ChaosReplicatedTable:
            THROW_ERROR_EXCEPTION_IF(
                !Meta->ChaosReplicatedTableMeta,
                "Invalid replicated table mapping row for replicated table %Qv: chaos replicated table meta cannot be null",
                Ref);
            break;
        default:
            THROW_ERROR_EXCEPTION(
                "Invalid replicated table mapping row for object %Qv: incompatible type %Qlv",
                Ref,
                *ObjectType);
    }
}

void Serialize(const TReplicatedTableMappingTableRow& row, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("object").Value(row.Ref)
            .Item("revision").Value(row.Revision)
            .Item("object_type").Value(row.ObjectType)
            .Item("meta").Value(row.Meta)
            .Item("synchronization_error").Value(row.SynchronizationError)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

template class TTableBase<TReplicatedTableMappingTableRow, NRecords::TReplicatedTableMappingDescriptor>;

TReplicatedTableMappingTable::TReplicatedTableMappingTable(TYPath path, IClientPtr client)
    : TTableBase<TReplicatedTableMappingTableRow, NRecords::TReplicatedTableMappingDescriptor>(std::move(path), std::move(client))
{ }

////////////////////////////////////////////////////////////////////////////////

TDynamicState::TDynamicState(
    const TQueueAgentDynamicStateConfigPtr& config,
    const IClientPtr& localClient,
    const TClientDirectoryPtr& clientDirectory)
    : Queues(New<TQueueTable>(config->Root, localClient))
    , Consumers(New<TConsumerTable>(config->Root, localClient))
    , QueueAgentObjectMapping(New<TQueueAgentObjectMappingTable>(config->Root, localClient))
    , Registrations(New<TConsumerRegistrationTable>(
        config->ConsumerRegistrationTablePath.GetPath(),
        GetRemoteClient(localClient, clientDirectory, config->ConsumerRegistrationTablePath.GetCluster())))
    , ReplicatedTableMapping(New<TReplicatedTableMappingTable>(
        config->ReplicatedTableMappingTablePath.GetPath(),
        GetRemoteClient(localClient, clientDirectory, config->ReplicatedTableMappingTablePath.GetCluster())))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
