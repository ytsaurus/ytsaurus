#include "dynamic_state.h"

#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/check_schema_compatibility.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NQueueAgent {

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NQueueClient;
using namespace NTableClient;
using namespace NYPath;
using namespace NApi;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueueAgentLogger;

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
};

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
        "Invoking select query (Query: %Qv)",
        query);

    return Client_->SelectRows(query)
        .Apply(BIND([&] (const TSelectRowsResult& result) {
            const auto& rowset = result.Rowset;
            return TRow::ParseRowRange(rowset->GetRows(), rowset->GetNameTable(), rowset->GetSchema());
        }));
}

template <class TRow>
TFuture<TTransactionCommitResult> TTableBase<TRow>::Insert(std::vector<TRow> rows) const
{
    return Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet)
        .Apply(BIND([rows = std::move(rows), path = Path_] (const ITransactionPtr& transaction) {
            auto rowset = TRow::InsertRowRange(rows);
            transaction->WriteRows(path, rowset->GetNameTable(), rowset->GetSharedRange());
            return transaction->Commit();
        }));
}

template <class TRow>
TFuture<TTransactionCommitResult> TTableBase<TRow>::Delete(std::vector<TRow> keys) const
{
    return Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet)
        .Apply(BIND([keys = std::move(keys), path = Path_] (const ITransactionPtr& transaction) {
            auto rowset = TRow::DeleteRowRange(keys);
            transaction->DeleteRows(path, rowset->GetNameTable(), rowset->GetSharedRange());
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
    TColumnSchema("auto_trim_policy", EValueType::String),
    TColumnSchema("queue_agent_stage", EValueType::String),
    TColumnSchema("synchronization_error", EValueType::Any),
});

////////////////////////////////////////////////////////////////////////////////

std::vector<TQueueTableRow> TQueueTableRow::ParseRowRange(
    TRange<TUnversionedRow> rows,
    const TNameTablePtr& nameTable,
    const TTableSchemaPtr& schema)
{
    std::vector<TQueueTableRow> typedRows;
    typedRows.reserve(rows.size());

    if (auto [compatibility, error] = CheckTableSchemaCompatibility(*schema, *TQueueTableDescriptor::Schema, /*ignoreSortOrder*/ true);
        compatibility != ESchemaCompatibility::FullyCompatible) {
        THROW_ERROR_EXCEPTION("Row range schema is incompatible with queue table row schema")
            << error;
    }

    auto clusterId = nameTable->FindId("cluster");
    auto pathId = nameTable->FindId("path");
    // Ensured by compatibility check above.
    YT_VERIFY(clusterId && pathId);

    auto objectTypeId = nameTable->FindId("object_type");
    auto rowRevisionId = nameTable->FindId("row_revision");
    auto revisionId = nameTable->FindId("revision");
    auto dynamicId = nameTable->FindId("dynamic");
    auto sortedId = nameTable->FindId("sorted");
    auto autoTrimPolicyId = nameTable->FindId("auto_trim_policy");
    auto queueAgentStageId = nameTable->FindId("queue_agent_stage");
    auto synchronizationErrorId = nameTable->FindId("synchronization_error");

    for (const auto& row : rows) {
        auto& typedRow = typedRows.emplace_back();
        typedRow.Queue = TCrossClusterReference{row[*clusterId].AsString(), row[*pathId].AsString()};

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

        if (auto autoTrimPolicy = findValue(autoTrimPolicyId)) {
            typedRow.AutoTrimPolicy = ParseEnum<EQueueAutoTrimPolicy>(autoTrimPolicy->AsStringBuf());
        }

        setSimpleOptional(queueAgentStageId, typedRow.QueueAgentStage);
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

        rowBuilder.AddValue(ToUnversionedValue(row.Queue.Cluster, rowBuffer, nameTable->GetIdOrThrow("cluster")));
        rowBuilder.AddValue(ToUnversionedValue(row.Queue.Path, rowBuffer, nameTable->GetIdOrThrow("path")));
        rowBuilder.AddValue(ToUnversionedValue(row.RowRevision, rowBuffer, nameTable->GetIdOrThrow("row_revision")));
        rowBuilder.AddValue(ToUnversionedValue(row.Revision, rowBuffer, nameTable->GetIdOrThrow("revision")));
        rowBuilder.AddValue(ToUnversionedValue(MapEnumToString(row.ObjectType), rowBuffer, nameTable->GetIdOrThrow("object_type")));
        rowBuilder.AddValue(ToUnversionedValue(row.Dynamic, rowBuffer, nameTable->GetIdOrThrow("dynamic")));
        rowBuilder.AddValue(ToUnversionedValue(row.Sorted, rowBuffer, nameTable->GetIdOrThrow("sorted")));
        rowBuilder.AddValue(ToUnversionedValue(MapEnumToString(row.AutoTrimPolicy), rowBuffer, nameTable->GetIdOrThrow("auto_trim_policy")));
        rowBuilder.AddValue(ToUnversionedValue(row.QueueAgentStage, rowBuffer, nameTable->GetIdOrThrow("queue_agent_stage")));
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
        rowBuilder.AddValue(MakeUnversionedStringValue(row.Queue.Cluster, nameTable->GetIdOrThrow("cluster")));
        rowBuilder.AddValue(MakeUnversionedStringValue(row.Queue.Path, nameTable->GetIdOrThrow("path")));

        rowsBuilder.AddRow(rowBuilder.FinishRow().Get());
    }

    return CreateRowset(TQueueTableDescriptor::Schema, rowsBuilder.Build());
}

std::vector<TString> TQueueTableRow::GetCypressAttributeNames()
{
    return {"attribute_revision", "type", "dynamic", "sorted", "auto_trim_policy", "queue_agent_stage"};
}

TQueueTableRow TQueueTableRow::FromAttributeDictionary(
    const TCrossClusterReference& queue,
    std::optional<TRowRevision> rowRevision,
    const IAttributeDictionaryPtr& cypressAttributes)
{
    return {
        .Queue = queue,
        .RowRevision = rowRevision,
        .Revision = cypressAttributes->Find<NHydra::TRevision>("attribute_revision"),
        .ObjectType = cypressAttributes->Find<EObjectType>("type"),
        .Dynamic = cypressAttributes->Find<bool>("dynamic"),
        .Sorted = cypressAttributes->Find<bool>("sorted"),
        .AutoTrimPolicy = cypressAttributes->Find<EQueueAutoTrimPolicy>("auto_trim_policy"),
        .QueueAgentStage = cypressAttributes->Find<TString>("queue_agent_stage"),
        .SynchronizationError = TError(),
    };
}

void Serialize(const TQueueTableRow& row, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("queue").Value(row.Queue)
            .Item("row_revision").Value(row.Revision)
            .Item("revision").Value(row.Revision)
            .Item("object_type").Value(row.ObjectType)
            .Item("dynamic").Value(row.Dynamic)
            .Item("sorted").Value(row.Sorted)
            .Item("auto_trim_policy").Value(row.AutoTrimPolicy)
            .Item("queue_agent_stage").Value(row.QueueAgentStage)
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
    TColumnSchema("target_cluster", EValueType::String),
    TColumnSchema("target_path", EValueType::String),
    TColumnSchema("object_type", EValueType::String),
    TColumnSchema("treat_as_queue_consumer", EValueType::Boolean),
    TColumnSchema("schema", EValueType::Any),
    TColumnSchema("vital", EValueType::Boolean),
    TColumnSchema("owner", EValueType::String),
    TColumnSchema("queue_agent_stage", EValueType::String),
    TColumnSchema("synchronization_error", EValueType::Any),
});

////////////////////////////////////////////////////////////////////////////////

std::vector<TConsumerTableRow> TConsumerTableRow::ParseRowRange(
    TRange<TUnversionedRow> rows,
    const TNameTablePtr& nameTable,
    const TTableSchemaPtr& schema)
{
    // TODO(max42): eliminate copy-paste?
    std::vector<TConsumerTableRow> typedRows;
    typedRows.reserve(rows.size());

    if (auto [compatibility, error] = CheckTableSchemaCompatibility(*schema, *TConsumerTableDescriptor::Schema, /*ignoreSortOrder*/ true);
        compatibility != ESchemaCompatibility::FullyCompatible) {
        THROW_ERROR_EXCEPTION("Row range schema is incompatible with consumer table row schema")
            << error;
    }

    auto clusterId = nameTable->FindId("cluster");
    auto pathId = nameTable->FindId("path");
    // Ensured by compatibility check above.
    YT_VERIFY(clusterId && pathId);

    auto rowRevisionId = nameTable->FindId("row_revision");
    auto revisionId = nameTable->FindId("revision");
    auto targetClusterId = nameTable->FindId("target_cluster");
    auto targetPathId = nameTable->FindId("target_path");
    auto objectTypeId = nameTable->FindId("object_type");
    auto treatAsQueueConsumerId = nameTable->FindId("treat_as_queue_consumer");
    auto schemaId = nameTable->FindId("schema");
    auto vitalId = nameTable->FindId("vital");
    auto ownerId = nameTable->FindId("owner");
    auto queueAgentStageId = nameTable->FindId("queue_agent_stage");
    auto synchronizationErrorId = nameTable->FindId("synchronization_error");

    for (const auto& row : rows) {
        auto& typedRow = typedRows.emplace_back();
        typedRow.Consumer = TCrossClusterReference{row[*clusterId].AsString(), row[*pathId].AsString()};

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

        if (auto targetCluster = findValue(targetClusterId), targetPath = findValue(targetPathId); targetCluster && targetPath) {
            typedRow.TargetQueue = TCrossClusterReference{targetCluster->AsString(), targetPath->AsString()};
        }

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

        setSimpleOptional(vitalId, typedRow.Vital);
        setSimpleOptional(ownerId, typedRow.Owner);
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

        rowBuilder.AddValue(ToUnversionedValue(row.Consumer.Cluster, rowBuffer, nameTable->GetIdOrThrow("cluster")));
        rowBuilder.AddValue(ToUnversionedValue(row.Consumer.Path, rowBuffer, nameTable->GetIdOrThrow("path")));
        rowBuilder.AddValue(ToUnversionedValue(row.RowRevision, rowBuffer, nameTable->GetIdOrThrow("row_revision")));
        rowBuilder.AddValue(ToUnversionedValue(row.Revision, rowBuffer, nameTable->GetIdOrThrow("revision")));

        std::optional<TString> targetQueueCluster;
        std::optional<TString> targetQueuePath;
        if (row.TargetQueue) {
            targetQueueCluster = row.TargetQueue->Cluster;
            targetQueuePath = row.TargetQueue->Path;
        }

        rowBuilder.AddValue(ToUnversionedValue(targetQueueCluster, rowBuffer, nameTable->GetIdOrThrow("target_cluster")));
        rowBuilder.AddValue(ToUnversionedValue(targetQueuePath, rowBuffer, nameTable->GetIdOrThrow("target_path")));
        rowBuilder.AddValue(ToUnversionedValue(MapEnumToString(row.ObjectType), rowBuffer, nameTable->GetIdOrThrow("object_type")));
        rowBuilder.AddValue(ToUnversionedValue(row.TreatAsQueueConsumer, rowBuffer, nameTable->GetIdOrThrow("treat_as_queue_consumer")));

        std::optional<TYsonString> schemaYson;
        if (row.Schema) {
            // Enclosing into a list is a workaround for storing YSON with top-level attributes.
            schemaYson = ConvertToYsonString(std::vector{row.Schema});
        }

        rowBuilder.AddValue(ToUnversionedValue(schemaYson, rowBuffer, nameTable->GetIdOrThrow("schema")));
        rowBuilder.AddValue(ToUnversionedValue(row.Vital, rowBuffer, nameTable->GetIdOrThrow("vital")));
        rowBuilder.AddValue(ToUnversionedValue(row.Owner, rowBuffer, nameTable->GetIdOrThrow("owner")));
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
        rowBuilder.AddValue(MakeUnversionedStringValue(row.Consumer.Cluster, nameTable->GetIdOrThrow("cluster")));
        rowBuilder.AddValue(MakeUnversionedStringValue(row.Consumer.Path, nameTable->GetIdOrThrow("path")));

        rowsBuilder.AddRow(rowBuilder.FinishRow().Get());
    }

    return CreateRowset(TConsumerTableDescriptor::Schema, rowsBuilder.Build());
}

std::vector<TString> TConsumerTableRow::GetCypressAttributeNames()
{
    return {"target_queue", "attribute_revision", "type", "treat_as_queue_consumer", "schema",
            "vital_queue_consumer", "owner", "queue_agent_stage"};
}

TConsumerTableRow TConsumerTableRow::FromAttributeDictionary(
    const TCrossClusterReference& consumer,
    std::optional<TRowRevision> rowRevision,
    const IAttributeDictionaryPtr& cypressAttributes)
{
    auto optionalTargetQueue = cypressAttributes->Find<TString>("target_queue");
    auto targetQueue = (optionalTargetQueue ? std::make_optional(TCrossClusterReference::FromString(*optionalTargetQueue)) : std::nullopt);
    return {
        .Consumer = consumer,
        .RowRevision = rowRevision,
        .TargetQueue = targetQueue,
        .Revision = cypressAttributes->Get<NHydra::TRevision>("attribute_revision"),
        .ObjectType = cypressAttributes->Get<EObjectType>("type"),
        .TreatAsQueueConsumer = cypressAttributes->Get<bool>("treat_as_queue_consumer", false),
        .Schema = cypressAttributes->Find<TTableSchema>("schema"),
        .Vital = cypressAttributes->Get<bool>("vital_queue_consumer", false),
        .Owner = cypressAttributes->Get<TString>("owner", ""),
        .QueueAgentStage = cypressAttributes->Find<TString>("queue_agent_stage"),
        .SynchronizationError = TError(),
    };
}

void Serialize(const TConsumerTableRow& row, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("consumer").Value(row.Consumer)
            .Item("row_revision").Value(row.RowRevision)
            .Item("revision").Value(row.Revision)
            .Item("target_queue").Value(row.TargetQueue)
            .Item("object_type").Value(row.ObjectType)
            .Item("treat_as_queue_consumer").Value(row.TreatAsQueueConsumer)
            .Item("schema").Value(row.Schema)
            .Item("vital").Value(row.Vital)
            .Item("owner").Value(row.Owner)
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

TDynamicState::TDynamicState(TYPath root, IClientPtr client)
    : Queues(New<TQueueTable>(root, client))
    , Consumers(New<TConsumerTable>(root, client))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
