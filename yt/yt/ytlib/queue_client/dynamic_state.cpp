#include "dynamic_state.h"
#include "private.h"
#include "config.h"

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/check_schema_compatibility.h>

#include <yt/yt/client/queue_client/config.h>

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

static const auto& Logger = QueueClientLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

template <class T>
std::optional<TString> MapEnumToString(const std::optional<T>& optionalValue)
{
    std::optional<TString> stringValue;
    if (optionalValue) {
        stringValue = FormatEnum(*optionalValue);
    }
    return stringValue;
}

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

template <class TRow>
TTableBase<TRow>::TTableBase(TYPath path, NApi::IClientPtr client)
    : Path_(std::move(path))
    , Client_(std::move(client))
{ }

template <class TRow>
TFuture<std::vector<TRow>> TTableBase<TRow>::Select(TStringBuf columns, TStringBuf where) const
{
    TString query = Format("%v from [%v] where %v", columns, Path_, where);

    YT_LOG_DEBUG(
        "Invoking select query (Query: %v)",
        query);

    return Client_->SelectRows(query)
        .Apply(BIND([&] (const TSelectRowsResult& result) {
            const auto& rowset = result.Rowset;
            return TRow::ParseRowRange(rowset->GetRows(), rowset->GetNameTable());
        }));
}

template <class TRow>
TFuture<TTransactionCommitResult> TTableBase<TRow>::Insert(std::vector<TRow> rows) const
{
    return Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet)
        .Apply(BIND([rows = std::move(rows), path = Path_] (const ITransactionPtr& transaction) {
            auto rowset = TRow::InsertRowRange(rows);
            transaction->WriteRows(path, rowset->GetNameTable(), rowset->GetRows(), {.RequireSyncReplica = false});
            return transaction->Commit();
        }));
}

template <class TRow>
TFuture<TTransactionCommitResult> TTableBase<TRow>::Delete(std::vector<TRow> keys) const
{
    return Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet)
        .Apply(BIND([keys = std::move(keys), path = Path_] (const ITransactionPtr& transaction) {
            auto rowset = TRow::DeleteRowRange(keys);
            transaction->DeleteRows(path, rowset->GetNameTable(), rowset->GetRows(), {.RequireSyncReplica = false});
            return transaction->Commit();
        }));
}

////////////////////////////////////////////////////////////////////////////////

struct TQueueTableDescriptor
{
    static constexpr TStringBuf Name = "queues";
    static NTableClient::TTableSchemaPtr Schema;
};

TTableSchemaPtr TQueueTableDescriptor::Schema = New<TTableSchema>(std::vector<TColumnSchema>{
    TColumnSchema("cluster", EValueType::String, ESortOrder::Ascending),
    TColumnSchema("path", EValueType::String, ESortOrder::Ascending),
    TColumnSchema("row_revision", EValueType::Uint64),
    TColumnSchema("revision", EValueType::Uint64),
    TColumnSchema("object_type", EValueType::String),
    TColumnSchema("dynamic", EValueType::Boolean),
    TColumnSchema("sorted", EValueType::Boolean),
    TColumnSchema("auto_trim_config", EValueType::Any),
    TColumnSchema("queue_agent_stage", EValueType::String),
    TColumnSchema("object_id", EValueType::String),
    TColumnSchema("synchronization_error", EValueType::Any),
});

////////////////////////////////////////////////////////////////////////////////

std::vector<TQueueTableRow> TQueueTableRow::ParseRowRange(
    TRange<TUnversionedRow> rows,
    const TNameTablePtr& nameTable)
{
    std::vector<TQueueTableRow> typedRows;
    typedRows.reserve(rows.size());

    auto clusterId = nameTable->GetIdOrThrow("cluster");
    auto pathId = nameTable->GetIdOrThrow("path");

    auto objectTypeId = nameTable->FindId("object_type");
    auto rowRevisionId = nameTable->FindId("row_revision");
    auto revisionId = nameTable->FindId("revision");
    auto dynamicId = nameTable->FindId("dynamic");
    auto sortedId = nameTable->FindId("sorted");
    auto autoTrimConfigId = nameTable->FindId("auto_trim_config");
    auto queueAgentStageId = nameTable->FindId("queue_agent_stage");
    auto objectIdFieldId = nameTable->FindId("object_id");
    auto synchronizationErrorId = nameTable->FindId("synchronization_error");

    for (const auto& row : rows) {
        auto& typedRow = typedRows.emplace_back();
        typedRow.Ref = TCrossClusterReference{row[clusterId].AsString(), row[pathId].AsString()};

        auto findValue = [&] (std::optional<int> id) -> std::optional<TUnversionedValue> {
            if (id && row[*id].Type != EValueType::Null) {
                return row[*id];
            }
            return std::nullopt;
        };

        auto setSimpleOptional = [&]<class T>(std::optional<int> id, std::optional<T>& valueToSet) {
            if (auto value = findValue(id)) {
                valueToSet = FromUnversionedValue<T>(*value);
            }
        };

        setSimpleOptional(rowRevisionId, typedRow.RowRevision);
        setSimpleOptional(revisionId, typedRow.Revision);

        if (auto type = findValue(objectTypeId)) {
            // TODO(max42): possible exception here is not handled well.
            typedRow.ObjectType = ParseEnum<EObjectType>(type->AsStringBuf());
        }

        setSimpleOptional(dynamicId, typedRow.Dynamic);
        setSimpleOptional(sortedId, typedRow.Sorted);

        if (auto autoTrimConfig = findValue(autoTrimConfigId)) {
            typedRow.AutoTrimConfig = ConvertTo<TQueueAutoTrimConfig>(TYsonStringBuf(autoTrimConfig->AsStringBuf()));
        } else {
            typedRow.AutoTrimConfig = TQueueAutoTrimConfig::Create();
        }

        setSimpleOptional(queueAgentStageId, typedRow.QueueAgentStage);
        setSimpleOptional(objectIdFieldId, typedRow.ObjectId);
        setSimpleOptional(synchronizationErrorId, typedRow.SynchronizationError);
    }

    return typedRows;
}

IUnversionedRowsetPtr TQueueTableRow::InsertRowRange(TRange<TQueueTableRow> rows)
{
    auto nameTable = TNameTable::FromSchema(*TQueueTableDescriptor::Schema);

    TUnversionedRowsBuilder rowsBuilder;
    for (const auto& row : rows) {
        auto rowBuffer = New<TRowBuffer>();
        TUnversionedRowBuilder rowBuilder;

        rowBuilder.AddValue(ToUnversionedValue(row.Ref.Cluster, rowBuffer, nameTable->GetIdOrThrow("cluster")));
        rowBuilder.AddValue(ToUnversionedValue(row.Ref.Path, rowBuffer, nameTable->GetIdOrThrow("path")));
        rowBuilder.AddValue(ToUnversionedValue(row.RowRevision, rowBuffer, nameTable->GetIdOrThrow("row_revision")));
        rowBuilder.AddValue(ToUnversionedValue(row.Revision, rowBuffer, nameTable->GetIdOrThrow("revision")));
        rowBuilder.AddValue(ToUnversionedValue(MapEnumToString(row.ObjectType), rowBuffer, nameTable->GetIdOrThrow("object_type")));
        rowBuilder.AddValue(ToUnversionedValue(row.Dynamic, rowBuffer, nameTable->GetIdOrThrow("dynamic")));
        rowBuilder.AddValue(ToUnversionedValue(row.Sorted, rowBuffer, nameTable->GetIdOrThrow("sorted")));

        std::optional<TYsonString> autoTrimConfigYson;
        if (row.AutoTrimConfig) {
            autoTrimConfigYson = ConvertToYsonString(row.AutoTrimConfig);
        }

        rowBuilder.AddValue(ToUnversionedValue(autoTrimConfigYson, rowBuffer, nameTable->GetIdOrThrow("auto_trim_config")));
        rowBuilder.AddValue(ToUnversionedValue(row.QueueAgentStage, rowBuffer, nameTable->GetIdOrThrow("queue_agent_stage")));
        rowBuilder.AddValue(ToUnversionedValue(row.ObjectId, rowBuffer, nameTable->GetIdOrThrow("object_id")));
        rowBuilder.AddValue(ToUnversionedValue(row.SynchronizationError, rowBuffer, nameTable->GetIdOrThrow("synchronization_error")));

        rowsBuilder.AddRow(rowBuilder.GetRow());
    }

    return CreateRowset(TQueueTableDescriptor::Schema, rowsBuilder.Build());
}

NApi::IUnversionedRowsetPtr TQueueTableRow::DeleteRowRange(TRange<TQueueTableRow> keys)
{
    auto nameTable = TNameTable::FromSchema(*TQueueTableDescriptor::Schema);

    TUnversionedRowsBuilder rowsBuilder;
    for (const auto& row : keys) {
        TUnversionedOwningRowBuilder rowBuilder;
        rowBuilder.AddValue(MakeUnversionedStringValue(row.Ref.Cluster, nameTable->GetIdOrThrow("cluster")));
        rowBuilder.AddValue(MakeUnversionedStringValue(row.Ref.Path, nameTable->GetIdOrThrow("path")));

        rowsBuilder.AddRow(rowBuilder.FinishRow().Get());
    }

    return CreateRowset(TQueueTableDescriptor::Schema, rowsBuilder.Build());
}

std::vector<TString> TQueueTableRow::GetCypressAttributeNames()
{
    return {
        "attribute_revision",
        "type",
        "dynamic",
        "sorted",
        "auto_trim_config",
        "queue_agent_stage",
        "id",
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
        .AutoTrimConfig = cypressAttributes->Find<TQueueAutoTrimConfig>("auto_trim_config"),
        .QueueAgentStage = cypressAttributes->Find<TString>("queue_agent_stage"),
        .ObjectId = cypressAttributes->Find<TObjectId>("id"),
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
            .Item("queue_agent_stage").Value(row.QueueAgentStage)
            .Item("object_id").Value(row.ObjectId)
            .Item("synchronization_error").Value(row.SynchronizationError)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

template class TTableBase<TQueueTableRow>;

TQueueTable::TQueueTable(TYPath root, IClientPtr client)
    : TTableBase<TQueueTableRow>(root + "/" + TQueueTableDescriptor::Name, std::move(client))
{ }

////////////////////////////////////////////////////////////////////////////////

struct TConsumerTableDescriptor
{
    static constexpr TStringBuf Name = "consumers";
    static NTableClient::TTableSchemaPtr Schema;
};

TTableSchemaPtr TConsumerTableDescriptor::Schema = New<TTableSchema>(std::vector<TColumnSchema>{
    TColumnSchema("cluster", EValueType::String, ESortOrder::Ascending),
    TColumnSchema("path", EValueType::String, ESortOrder::Ascending),
    TColumnSchema("row_revision", EValueType::Uint64),
    TColumnSchema("revision", EValueType::Uint64),
    TColumnSchema("object_type", EValueType::String),
    TColumnSchema("treat_as_queue_consumer", EValueType::Boolean),
    TColumnSchema("schema", EValueType::Any),
    TColumnSchema("queue_agent_stage", EValueType::String),
    TColumnSchema("synchronization_error", EValueType::Any),
});

////////////////////////////////////////////////////////////////////////////////

std::vector<TConsumerTableRow> TConsumerTableRow::ParseRowRange(
    TRange<TUnversionedRow> rows,
    const TNameTablePtr& nameTable)
{
    // TODO(max42): eliminate copy-paste?
    std::vector<TConsumerTableRow> typedRows;
    typedRows.reserve(rows.size());

    auto clusterId = nameTable->GetIdOrThrow("cluster");
    auto pathId = nameTable->GetIdOrThrow("path");

    auto rowRevisionId = nameTable->FindId("row_revision");
    auto revisionId = nameTable->FindId("revision");
    auto objectTypeId = nameTable->FindId("object_type");
    auto treatAsQueueConsumerId = nameTable->FindId("treat_as_queue_consumer");
    auto schemaId = nameTable->FindId("schema");
    auto queueAgentStageId = nameTable->FindId("queue_agent_stage");
    auto synchronizationErrorId = nameTable->FindId("synchronization_error");

    for (const auto& row : rows) {
        auto& typedRow = typedRows.emplace_back();
        typedRow.Ref = TCrossClusterReference{row[clusterId].AsString(), row[pathId].AsString()};

        auto findValue = [&] (std::optional<int> id) -> std::optional<TUnversionedValue> {
            for (const auto& value : row) {
                if (id && value.Type != EValueType::Null && value.Id == *id) {
                    return value;
                }
            }
            return std::nullopt;
        };

        auto setSimpleOptional = [&]<class T>(std::optional<int> id, std::optional<T>& valueToSet) {
            if (auto value = findValue(id)) {
                valueToSet = FromUnversionedValue<T>(*value);
            }
        };

        setSimpleOptional(rowRevisionId, typedRow.RowRevision);
        setSimpleOptional(revisionId, typedRow.Revision);

        if (auto type = findValue(objectTypeId)) {
            // TODO(max42): possible exception here is not handled well.
            typedRow.ObjectType = ParseEnum<EObjectType>(type->AsStringBuf());
        }

        setSimpleOptional(treatAsQueueConsumerId, typedRow.TreatAsQueueConsumer);

        if (auto schemaValue = findValue(schemaId)) {
            auto workaroundVector = ConvertTo<std::vector<TTableSchema>>(TYsonStringBuf(schemaValue->AsStringBuf()));
            YT_VERIFY(workaroundVector.size() == 1);
            typedRow.Schema = workaroundVector.back();
        }

        setSimpleOptional(queueAgentStageId, typedRow.QueueAgentStage);
        setSimpleOptional(synchronizationErrorId, typedRow.SynchronizationError);
    }

    return typedRows;
}

IUnversionedRowsetPtr TConsumerTableRow::InsertRowRange(TRange<TConsumerTableRow> rows)
{
    auto nameTable = TNameTable::FromSchema(*TConsumerTableDescriptor::Schema);

    TUnversionedRowsBuilder rowsBuilder;
    for (const auto& row : rows) {
        auto rowBuffer = New<TRowBuffer>();
        TUnversionedRowBuilder rowBuilder;

        rowBuilder.AddValue(ToUnversionedValue(row.Ref.Cluster, rowBuffer, nameTable->GetIdOrThrow("cluster")));
        rowBuilder.AddValue(ToUnversionedValue(row.Ref.Path, rowBuffer, nameTable->GetIdOrThrow("path")));
        rowBuilder.AddValue(ToUnversionedValue(row.RowRevision, rowBuffer, nameTable->GetIdOrThrow("row_revision")));
        rowBuilder.AddValue(ToUnversionedValue(row.Revision, rowBuffer, nameTable->GetIdOrThrow("revision")));
        rowBuilder.AddValue(ToUnversionedValue(MapEnumToString(row.ObjectType), rowBuffer, nameTable->GetIdOrThrow("object_type")));
        rowBuilder.AddValue(ToUnversionedValue(row.TreatAsQueueConsumer, rowBuffer, nameTable->GetIdOrThrow("treat_as_queue_consumer")));

        std::optional<TYsonString> schemaYson;
        if (row.Schema) {
            // Enclosing into a list is a workaround for storing YSON with top-level attributes.
            schemaYson = ConvertToYsonString(std::vector{row.Schema});
        }

        rowBuilder.AddValue(ToUnversionedValue(schemaYson, rowBuffer, nameTable->GetIdOrThrow("schema")));
        rowBuilder.AddValue(ToUnversionedValue(row.QueueAgentStage, rowBuffer, nameTable->GetIdOrThrow("queue_agent_stage")));
        rowBuilder.AddValue(ToUnversionedValue(row.SynchronizationError, rowBuffer, nameTable->GetIdOrThrow("synchronization_error")));

        rowsBuilder.AddRow(rowBuilder.GetRow());
    }

    return CreateRowset(TConsumerTableDescriptor::Schema, rowsBuilder.Build());
}

NApi::IUnversionedRowsetPtr TConsumerTableRow::DeleteRowRange(TRange<TConsumerTableRow> keys)
{
    auto nameTable = TNameTable::FromSchema(*TConsumerTableDescriptor::Schema);

    TUnversionedRowsBuilder rowsBuilder;
    for (const auto& row : keys) {
        TUnversionedOwningRowBuilder rowBuilder;
        rowBuilder.AddValue(MakeUnversionedStringValue(row.Ref.Cluster, nameTable->GetIdOrThrow("cluster")));
        rowBuilder.AddValue(MakeUnversionedStringValue(row.Ref.Path, nameTable->GetIdOrThrow("path")));

        rowsBuilder.AddRow(rowBuilder.FinishRow().Get());
    }

    return CreateRowset(TConsumerTableDescriptor::Schema, rowsBuilder.Build());
}

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

template class TTableBase<TConsumerTableRow>;

TConsumerTable::TConsumerTable(TYPath root, IClientPtr client)
    : TTableBase<TConsumerTableRow>(root + "/" + TConsumerTableDescriptor::Name, std::move(client))
{ }

////////////////////////////////////////////////////////////////////////////////

struct TQueueAgentObjectMappingTableDescriptor
{
    static constexpr TStringBuf Name = "queue_agent_object_mapping";
    static NTableClient::TTableSchemaPtr Schema;
};

TTableSchemaPtr TQueueAgentObjectMappingTableDescriptor::Schema = New<TTableSchema>(std::vector<TColumnSchema>{
    TColumnSchema("object", EValueType::String, ESortOrder::Ascending),
    TColumnSchema("host", EValueType::String),
});

std::vector<TQueueAgentObjectMappingTableRow> TQueueAgentObjectMappingTableRow::ParseRowRange(
    TRange<TUnversionedRow> rows,
    const TNameTablePtr& nameTable)
{
    // TODO(max42): eliminate copy-paste?
    std::vector<TQueueAgentObjectMappingTableRow> typedRows;
    typedRows.reserve(rows.size());

    auto objectId = nameTable->GetIdOrThrow("object");
    auto hostId = nameTable->GetIdOrThrow("host");

    for (const auto& row : rows) {
        auto& typedRow = typedRows.emplace_back();
        typedRow.Object = TCrossClusterReference::FromString(FromUnversionedValue<TString>(row[objectId]));
        typedRow.QueueAgentHost = FromUnversionedValue<TString>(row[hostId]);
    }

    return typedRows;
}

IUnversionedRowsetPtr TQueueAgentObjectMappingTableRow::InsertRowRange(TRange<TQueueAgentObjectMappingTableRow> rows)
{
    auto nameTable = TNameTable::FromSchema(*TQueueAgentObjectMappingTableDescriptor::Schema);

    TUnversionedRowsBuilder rowsBuilder;
    for (const auto& row : rows) {
        auto rowBuffer = New<TRowBuffer>();
        TUnversionedRowBuilder rowBuilder;

        rowBuilder.AddValue(ToUnversionedValue(ToString(row.Object), rowBuffer, nameTable->GetIdOrThrow("object")));
        rowBuilder.AddValue(ToUnversionedValue(row.QueueAgentHost, rowBuffer, nameTable->GetIdOrThrow("host")));

        rowsBuilder.AddRow(rowBuilder.GetRow());
    }

    return CreateRowset(TQueueAgentObjectMappingTableDescriptor::Schema, rowsBuilder.Build());
}

NApi::IUnversionedRowsetPtr TQueueAgentObjectMappingTableRow::DeleteRowRange(TRange<TQueueAgentObjectMappingTableRow> keys)
{
    auto nameTable = TNameTable::FromSchema(*TQueueAgentObjectMappingTableDescriptor::Schema);

    TUnversionedRowsBuilder rowsBuilder;
    for (const auto& row : keys) {
        TUnversionedOwningRowBuilder rowBuilder;
        rowBuilder.AddValue(MakeUnversionedStringValue(ToString(row.Object), nameTable->GetIdOrThrow("object")));

        rowsBuilder.AddRow(rowBuilder.FinishRow().Get());
    }

    return CreateRowset(TQueueAgentObjectMappingTableDescriptor::Schema, rowsBuilder.Build());
}

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

template class TTableBase<TQueueAgentObjectMappingTableRow>;

TQueueAgentObjectMappingTable::TQueueAgentObjectMappingTable(TYPath root, IClientPtr client)
    : TTableBase<TQueueAgentObjectMappingTableRow>(root + "/" + TQueueAgentObjectMappingTableDescriptor::Name, std::move(client))
{ }

////////////////////////////////////////////////////////////////////////////////

struct TConsumerRegistrationTableDescriptor
{
    static constexpr TStringBuf Name = "consumer_registrations";
    static NTableClient::TTableSchemaPtr Schema;
};

TTableSchemaPtr TConsumerRegistrationTableDescriptor::Schema = New<TTableSchema>(std::vector<TColumnSchema>{
    TColumnSchema("queue_cluster", EValueType::String, ESortOrder::Ascending),
    TColumnSchema("queue_path", EValueType::String, ESortOrder::Ascending),
    TColumnSchema("consumer_cluster", EValueType::String, ESortOrder::Ascending),
    TColumnSchema("consumer_path", EValueType::String, ESortOrder::Ascending),
    TColumnSchema("vital", EValueType::Boolean),
    TColumnSchema("partitions", EValueType::Any),
});

std::vector<TConsumerRegistrationTableRow> TConsumerRegistrationTableRow::ParseRowRange(
    TRange<TUnversionedRow> rows,
    const TNameTablePtr& nameTable)
{
    // TODO(max42): eliminate copy-paste?
    std::vector<TConsumerRegistrationTableRow> typedRows;
    typedRows.reserve(rows.size());

    auto queueClusterId = nameTable->GetIdOrThrow("queue_cluster");
    auto queuePathId = nameTable->GetIdOrThrow("queue_path");
    auto consumerClusterId = nameTable->GetIdOrThrow("consumer_cluster");
    auto consumerPathId = nameTable->GetIdOrThrow("consumer_path");

    auto vitalId = nameTable->FindId("vital");
    auto partitionsId = nameTable->FindId("partitions");

    for (const auto& row : rows) {
        auto& typedRow = typedRows.emplace_back();
        // TODO(max42): mark all relevant fields in schemas of dynamic state tables as required.
        typedRow.Queue = TCrossClusterReference{row[queueClusterId].AsString(), row[queuePathId].AsString()};
        typedRow.Consumer = TCrossClusterReference{row[consumerClusterId].AsString(), row[consumerPathId].AsString()};

        auto findValue = [&] (std::optional<int> id) -> std::optional<TUnversionedValue> {
            if (id && row[*id].Type != EValueType::Null) {
                return row[*id];
            }
            return std::nullopt;
        };

        auto setSimpleOptional = [&]<class T>(std::optional<int> id, std::optional<T>& valueToSet) {
            if (auto value = findValue(id)) {
                valueToSet = FromUnversionedValue<T>(*value);
            }
        };

        if (auto value = findValue(vitalId)) {
            YT_VERIFY(value->Type == EValueType::Boolean);
            typedRow.Vital = FromUnversionedValue<bool>(*value);
        } else {
            typedRow.Vital = false;
        }

        setSimpleOptional(partitionsId, typedRow.Partitions);
    }

    return typedRows;
}

IUnversionedRowsetPtr TConsumerRegistrationTableRow::InsertRowRange(TRange<TConsumerRegistrationTableRow> rows)
{
    auto nameTable = TNameTable::FromSchema(*TConsumerRegistrationTableDescriptor::Schema);

    TUnversionedRowsBuilder rowsBuilder;
    for (const auto& row : rows) {
        auto rowBuffer = New<TRowBuffer>();
        TUnversionedRowBuilder rowBuilder;

        rowBuilder.AddValue(ToUnversionedValue(row.Queue.Cluster, rowBuffer, nameTable->GetIdOrThrow("queue_cluster")));
        rowBuilder.AddValue(ToUnversionedValue(row.Queue.Path, rowBuffer, nameTable->GetIdOrThrow("queue_path")));
        rowBuilder.AddValue(ToUnversionedValue(row.Consumer.Cluster, rowBuffer, nameTable->GetIdOrThrow("consumer_cluster")));
        rowBuilder.AddValue(ToUnversionedValue(row.Consumer.Path, rowBuffer, nameTable->GetIdOrThrow("consumer_path")));
        rowBuilder.AddValue(ToUnversionedValue(row.Vital, rowBuffer, nameTable->GetIdOrThrow("vital")));
        rowBuilder.AddValue(ToUnversionedValue(row.Partitions, rowBuffer, nameTable->GetIdOrThrow("partitions")));

        rowsBuilder.AddRow(rowBuilder.GetRow());
    }

    return CreateRowset(TConsumerRegistrationTableDescriptor::Schema, rowsBuilder.Build());
}

NApi::IUnversionedRowsetPtr TConsumerRegistrationTableRow::DeleteRowRange(TRange<TConsumerRegistrationTableRow> keys)
{
    auto nameTable = TNameTable::FromSchema(*TConsumerRegistrationTableDescriptor::Schema);

    TUnversionedRowsBuilder rowsBuilder;
    for (const auto& row : keys) {
        TUnversionedOwningRowBuilder rowBuilder;
        rowBuilder.AddValue(MakeUnversionedStringValue(row.Queue.Cluster, nameTable->GetIdOrThrow("queue_cluster")));
        rowBuilder.AddValue(MakeUnversionedStringValue(row.Queue.Path, nameTable->GetIdOrThrow("queue_path")));
        rowBuilder.AddValue(MakeUnversionedStringValue(row.Consumer.Cluster, nameTable->GetIdOrThrow("consumer_cluster")));
        rowBuilder.AddValue(MakeUnversionedStringValue(row.Consumer.Path, nameTable->GetIdOrThrow("consumer_path")));

        rowsBuilder.AddRow(rowBuilder.FinishRow().Get());
    }

    return CreateRowset(TConsumerRegistrationTableDescriptor::Schema, rowsBuilder.Build());
}

////////////////////////////////////////////////////////////////////////////////

template class TTableBase<TConsumerRegistrationTableRow>;

TConsumerRegistrationTable::TConsumerRegistrationTable(TYPath path, IClientPtr client)
    : TTableBase<TConsumerRegistrationTableRow>(std::move(path), std::move(client))
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

struct TReplicatedTableMappingTableDescriptor
{
    static constexpr TStringBuf Name = "replicated_table_mapping";
    static NTableClient::TTableSchemaPtr Schema;
};

TTableSchemaPtr TReplicatedTableMappingTableDescriptor::Schema = New<TTableSchema>(std::vector<TColumnSchema>{
    TColumnSchema("cluster", EValueType::String, ESortOrder::Ascending),
    TColumnSchema("path", EValueType::String, ESortOrder::Ascending),
    TColumnSchema("revision", EValueType::Uint64),
    TColumnSchema("object_type", EValueType::String),
    TColumnSchema("meta", EValueType::Any),
    TColumnSchema("synchronization_error", EValueType::Any),
});

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

std::vector<TReplicatedTableMappingTableRow> TReplicatedTableMappingTableRow::ParseRowRange(
    TRange<TUnversionedRow> rows,
    const TNameTablePtr& nameTable)
{
    std::vector<TReplicatedTableMappingTableRow> typedRows;
    typedRows.reserve(rows.size());

    auto clusterId = nameTable->GetIdOrThrow("cluster");
    auto pathId = nameTable->GetIdOrThrow("path");

    auto objectTypeId = nameTable->FindId("object_type");
    auto revisionId = nameTable->FindId("revision");
    auto metaId = nameTable->FindId("meta");
    auto synchronizationErrorId = nameTable->FindId("synchronization_error");

    for (const auto& row : rows) {
        auto& typedRow = typedRows.emplace_back();
        typedRow.Ref = TCrossClusterReference{row[clusterId].AsString(), row[pathId].AsString()};

        auto findValue = [&] (std::optional<int> id) -> std::optional<TUnversionedValue> {
            if (id && row[*id].Type != EValueType::Null) {
                return row[*id];
            }
            return std::nullopt;
        };

        auto setSimpleOptional = [&]<class T>(std::optional<int> id, std::optional<T>& valueToSet) {
            if (auto value = findValue(id)) {
                valueToSet = FromUnversionedValue<T>(*value);
            }
        };

        setSimpleOptional(revisionId, typedRow.Revision);

        if (auto type = findValue(objectTypeId)) {
            // TODO(max42): possible exception here is not handled well.
            typedRow.ObjectType = ParseEnum<EObjectType>(type->AsStringBuf());
        }

        if (auto meta = findValue(metaId)) {
            typedRow.Meta = ConvertTo<TGenericReplicatedTableMetaPtr>(TYsonStringBuf(meta->AsStringBuf()));
        }

        setSimpleOptional(synchronizationErrorId, typedRow.SynchronizationError);
    }

    return typedRows;
}

IUnversionedRowsetPtr TReplicatedTableMappingTableRow::InsertRowRange(TRange<TReplicatedTableMappingTableRow> rows)
{
    auto nameTable = TNameTable::FromSchema(*TReplicatedTableMappingTableDescriptor::Schema);

    TUnversionedRowsBuilder rowsBuilder;
    for (const auto& row : rows) {
        auto rowBuffer = New<TRowBuffer>();
        TUnversionedRowBuilder rowBuilder;

        rowBuilder.AddValue(ToUnversionedValue(row.Ref.Cluster, rowBuffer, nameTable->GetIdOrThrow("cluster")));
        rowBuilder.AddValue(ToUnversionedValue(row.Ref.Path, rowBuffer, nameTable->GetIdOrThrow("path")));
        rowBuilder.AddValue(ToUnversionedValue(row.Revision, rowBuffer, nameTable->GetIdOrThrow("revision")));
        rowBuilder.AddValue(ToUnversionedValue(MapEnumToString(row.ObjectType), rowBuffer, nameTable->GetIdOrThrow("object_type")));

        std::optional<TYsonString> metaYson;
        if (row.Meta) {
            metaYson = ConvertToYsonString(row.Meta);
        }
        rowBuilder.AddValue(ToUnversionedValue(metaYson, rowBuffer, nameTable->GetIdOrThrow("meta")));

        rowBuilder.AddValue(ToUnversionedValue(row.SynchronizationError, rowBuffer, nameTable->GetIdOrThrow("synchronization_error")));

        rowsBuilder.AddRow(rowBuilder.GetRow());
    }

    return CreateRowset(TReplicatedTableMappingTableDescriptor::Schema, rowsBuilder.Build());
}


NApi::IUnversionedRowsetPtr TReplicatedTableMappingTableRow::DeleteRowRange(TRange<TReplicatedTableMappingTableRow> keys)
{
    auto nameTable = TNameTable::FromSchema(*TReplicatedTableMappingTableDescriptor::Schema);

    TUnversionedRowsBuilder rowsBuilder;
    for (const auto& row : keys) {
        TUnversionedOwningRowBuilder rowBuilder;
        rowBuilder.AddValue(MakeUnversionedStringValue(row.Ref.Cluster, nameTable->GetIdOrThrow("cluster")));
        rowBuilder.AddValue(MakeUnversionedStringValue(row.Ref.Path, nameTable->GetIdOrThrow("path")));

        rowsBuilder.AddRow(rowBuilder.FinishRow().Get());
    }

    return CreateRowset(TConsumerRegistrationTableDescriptor::Schema, rowsBuilder.Build());
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

template class TTableBase<TReplicatedTableMappingTableRow>;

TReplicatedTableMappingTable::TReplicatedTableMappingTable(TYPath path, IClientPtr client)
    : TTableBase<TReplicatedTableMappingTableRow>(std::move(path), std::move(client))
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
