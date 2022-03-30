#include "dynamic_state.h"

#include <yt/yt/ytlib/table_client/schema.h>

#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/ytree/fluent.h>

#include <util/string/split.h>

namespace NYT::NQueueAgent {

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NYPath;
using namespace NApi;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueueAgentLogger;

////////////////////////////////////////////////////////////////////////////////

bool TCrossClusterReference::operator==(const TCrossClusterReference& other) const
{
    return Cluster == other.Cluster && Path == other.Path;
}

bool TCrossClusterReference::operator<(const TCrossClusterReference& other) const
{
    return std::tie(Cluster, Path) < std::tie(other.Cluster, other.Path);
}

TCrossClusterReference TCrossClusterReference::FromString(TStringBuf path)
{
    TCrossClusterReference result;
    if (!StringSplitter(path).Split(':').Limit(2).TryCollectInto(&result.Cluster, &result.Path)) {
        THROW_ERROR_EXCEPTION("Ill-formed cross-cluster reference %Qv", path);
    }
    return result;
}

TString ToString(const TCrossClusterReference& queueRef)
{
    return Format("%v:%v", queueRef.Cluster, queueRef.Path);
}

void Serialize(const TCrossClusterReference& queueRef, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).Value(ToString(queueRef));
}

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
    return Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet).Apply(BIND([rows = std::move(rows), path = Path_] (const ITransactionPtr& transaction) {
        auto rowset = TRow::InsertRowRange(rows);
        transaction->WriteRows(path, rowset->GetNameTable(), rowset->GetSharedRange());
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
});

////////////////////////////////////////////////////////////////////////////////

std::vector<TQueueTableRow> TQueueTableRow::ParseRowRange(TRange<TUnversionedRow> rows, TNameTablePtr nameTable, const TTableSchema& schema)
{
    std::vector<TQueueTableRow> typedRows;
    typedRows.reserve(rows.size());

    if (auto [compatibility, error] = CheckTableSchemaCompatibility(schema, *TQueueTableDescriptor::Schema, /*ignoreSortOrder*/ true);
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

    for (const auto& row : rows) {
        auto& typedRow = typedRows.emplace_back();
        typedRow.Queue = TCrossClusterReference{row[*clusterId].AsString(), row[*pathId].AsString()};

        auto findValue = [&] (std::optional<int> id) -> std::optional<TUnversionedValue> {
            if (id && row[*id].Type != EValueType::Null) {
                return row[*id];
            }
            return std::nullopt;
        };

        if (auto rowRevision = findValue(rowRevisionId)) {
            typedRow.RowRevision = rowRevision->Data.Uint64;
        }
        if (auto revision = findValue(revisionId)) {
            typedRow.Revision = revision->Data.Uint64;
        }
        if (auto type = findValue(objectTypeId)) {
            // TODO(max42): possible exception here is not handled well.
            typedRow.ObjectType = ParseEnum<EObjectType>(type->AsStringBuf());
        }
        if (auto dynamic = findValue(dynamicId)) {
            typedRow.Dynamic = dynamic->Data.Boolean;
        }
        if (auto sorted = findValue(sortedId)) {
            typedRow.Sorted = sorted->Data.Boolean;
        }
    }

    return typedRows;
}

IUnversionedRowsetPtr TQueueTableRow::InsertRowRange(TRange<TQueueTableRow> rows)
{
    auto nameTable = TNameTable::FromSchema(*TQueueTableDescriptor::Schema);

    TUnversionedRowsBuilder rowsBuilder;
    for (const auto& row : rows) {
        TUnversionedOwningRowBuilder rowBuilder;
        rowBuilder.AddValue(MakeUnversionedStringValue(row.Queue.Cluster, nameTable->GetIdOrThrow("cluster")));
        rowBuilder.AddValue(MakeUnversionedStringValue(row.Queue.Path, nameTable->GetIdOrThrow("path")));
        if (row.RowRevision) {
            rowBuilder.AddValue(MakeUnversionedUint64Value(*row.RowRevision, nameTable->GetIdOrThrow("row_revision")));
        }
        if (row.Revision) {
            rowBuilder.AddValue(MakeUnversionedUint64Value(*row.Revision, nameTable->GetIdOrThrow("revision")));
        }
        if (row.ObjectType) {
            rowBuilder.AddValue(MakeUnversionedStringValue(FormatEnum<EObjectType>(*row.ObjectType), nameTable->GetIdOrThrow("object_type")));
        }
        if (row.Dynamic) {
            rowBuilder.AddValue(MakeUnversionedBooleanValue(*row.Dynamic, nameTable->GetIdOrThrow("dynamic")));
        }
        if (row.Sorted) {
            rowBuilder.AddValue(MakeUnversionedBooleanValue(*row.Sorted, nameTable->GetIdOrThrow("sorted")));
        }
        rowsBuilder.AddRow(rowBuilder.FinishRow().Get());
    }

    return CreateRowset(TQueueTableDescriptor::Schema, rowsBuilder.Build());
}

std::vector<TString> TQueueTableRow::GetCypressAttributeNames()
{
    return {"revision", "type", "dynamic", "sorted"};
}

TQueueTableRow TQueueTableRow::FromAttributeDictionary(
    const TCrossClusterReference& queue,
    std::optional<TRowRevision> rowRevision,
    const IAttributeDictionaryPtr& cypressAttributes)
{
    return {
        .Queue = queue,
        .RowRevision = rowRevision,
        .Revision = cypressAttributes->Find<NHydra::TRevision>("revision"),
        .ObjectType = cypressAttributes->Find<EObjectType>("type"),
        .Dynamic = cypressAttributes->Find<bool>("dynamic"),
        .Sorted = cypressAttributes->Find<bool>("sorted"),
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
});

////////////////////////////////////////////////////////////////////////////////

std::vector<TConsumerTableRow> TConsumerTableRow::ParseRowRange(TRange<TUnversionedRow> rows, TNameTablePtr nameTable, const TTableSchema& schema)
{
    // TODO(max42): eliminate copy-paste?
    std::vector<TConsumerTableRow> typedRows;
    typedRows.reserve(rows.size());

    if (auto [compatibility, error] = CheckTableSchemaCompatibility(schema, *TConsumerTableDescriptor::Schema, /*ignoreSortOrder*/ true);
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

    for (const auto& row : rows) {
        auto& typedRow = typedRows.emplace_back();
        typedRow.Consumer = TCrossClusterReference{row[*clusterId].AsString(), row[*pathId].AsString()};

        auto findValue = [&] (std::optional<int> id) -> std::optional<TUnversionedValue> {
            if (id && row[*id].Type != EValueType::Null) {
                return row[*id];
            }
            return std::nullopt;
        };

        if (auto rowRevision = findValue(rowRevisionId)) {
            typedRow.RowRevision = rowRevision->Data.Uint64;
        }
        if (auto revision = findValue(revisionId)) {
            typedRow.Revision = revision->Data.Uint64;
        }

        if (auto targetCluster = findValue(targetClusterId), targetPath = findValue(targetPathId); targetCluster && targetPath) {
            typedRow.TargetQueue = TCrossClusterReference{targetCluster->AsString(), targetPath->AsString()};
        }

        if (auto type = findValue(objectTypeId)) {
            // TODO(max42): possible exception here is not handled well.
            typedRow.ObjectType = ParseEnum<EObjectType>(type->AsStringBuf());
        }
        if (auto treatAsQueueConsumer = findValue(treatAsQueueConsumerId)) {
            typedRow.TreatAsQueueConsumer = treatAsQueueConsumer->Data.Boolean;
        }
        if (auto schema = findValue(schemaId)) {
            auto workaroundVector = ConvertTo<std::vector<TTableSchema>>(TYsonStringBuf(schema->AsStringBuf()));
            YT_VERIFY(workaroundVector.size() == 1);
            typedRow.Schema = workaroundVector.back();
        }
        if (auto vital = findValue(vitalId)) {
            typedRow.Vital = vital->Data.Boolean;
        }
        if (auto owner = findValue(ownerId)) {
            typedRow.Owner = owner->AsString();
        }
    }

    return typedRows;
}

IUnversionedRowsetPtr TConsumerTableRow::InsertRowRange(TRange<TConsumerTableRow> rows)
{
    auto nameTable = TNameTable::FromSchema(*TConsumerTableDescriptor::Schema);

    TUnversionedRowsBuilder rowsBuilder;
    for (const auto& row : rows) {
        TUnversionedOwningRowBuilder rowBuilder;
        rowBuilder.AddValue(MakeUnversionedStringValue(row.Consumer.Cluster, nameTable->GetIdOrThrow("cluster")));
        rowBuilder.AddValue(MakeUnversionedStringValue(row.Consumer.Path, nameTable->GetIdOrThrow("path")));
        if (row.RowRevision) {
            rowBuilder.AddValue(MakeUnversionedUint64Value(*row.RowRevision, nameTable->GetIdOrThrow("row_revision")));
        }
        if (row.Revision) {
            rowBuilder.AddValue(MakeUnversionedUint64Value(*row.Revision, nameTable->GetIdOrThrow("revision")));
        }
        if (row.TargetQueue) {
            rowBuilder.AddValue(MakeUnversionedStringValue(row.TargetQueue->Cluster, nameTable->GetIdOrThrow("target_cluster")));
            rowBuilder.AddValue(MakeUnversionedStringValue(row.TargetQueue->Path, nameTable->GetIdOrThrow("target_path")));
        }
        if (row.ObjectType) {
            rowBuilder.AddValue(MakeUnversionedStringValue(FormatEnum(*row.ObjectType), nameTable->GetIdOrThrow("object_type")));
        }
        if (row.TreatAsQueueConsumer) {
            rowBuilder.AddValue(MakeUnversionedBooleanValue(*row.TreatAsQueueConsumer, nameTable->GetIdOrThrow("treat_as_queue_consumer")));
        }
        TYsonString schemaYson;
        if (row.Schema) {
            // Enclosing into a list is a workaround for storing YSON with top-level attributes.
            schemaYson = ConvertToYsonString(std::vector<TTableSchema>{*row.Schema});
            rowBuilder.AddValue(MakeUnversionedAnyValue(schemaYson.AsStringBuf(), nameTable->GetIdOrThrow("schema")));
        }
        if (row.Vital) {
            rowBuilder.AddValue(MakeUnversionedBooleanValue(*row.Vital, nameTable->GetIdOrThrow("vital")));
        }
        if (row.Owner) {
            rowBuilder.AddValue(MakeUnversionedStringValue(*row.Owner, nameTable->GetIdOrThrow("owner")));
        }

        rowsBuilder.AddRow(rowBuilder.FinishRow().Get());
    }

    return CreateRowset(TConsumerTableDescriptor::Schema, rowsBuilder.Build());
}

std::vector<TString> TConsumerTableRow::GetCypressAttributeNames()
{
    return {"target_queue", "revision", "type", "treat_as_queue_consumer", "schema", "vital_queue_consumer", "owner"};
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
        .Revision = cypressAttributes->Get<NHydra::TRevision>("revision"),
        .ObjectType = cypressAttributes->Get<EObjectType>("type"),
        .TreatAsQueueConsumer = cypressAttributes->Get<bool>("treat_as_queue_consumer", false),
        .Schema = cypressAttributes->Find<TTableSchema>("schema"),
        .Vital = cypressAttributes->Get<bool>("vital_queue_consumer", false),
        .Owner = cypressAttributes->Get<TString>("owner", ""),
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

size_t THash<NYT::NQueueAgent::TCrossClusterReference>::operator()(const NYT::NQueueAgent::TCrossClusterReference& queueRef) const
{
    using NYT::HashCombine;

    size_t result = 0;
    HashCombine(result, queueRef.Cluster);
    HashCombine(result, queueRef.Path);
    return result;
}
