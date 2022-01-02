#include "state.h"

#include <yt/yt/ytlib/table_client/schema.h>

#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NQueueAgent {

using namespace NObjectClient;
using namespace NTableClient;
using namespace NYPath;
using namespace NApi;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueueAgentLogger;

////////////////////////////////////////////////////////////////////////////////

template <class TRow, class TId>
TTableBase<TRow, TId>::TTableBase(TYPath path, NApi::IClientPtr client)
    : Path_(std::move(path))
    , Client_(std::move(client))
{ }

template <class TRow, class TId>
TFuture<std::vector<TRow>> TTableBase<TRow, TId>::Select(TStringBuf columns, TStringBuf where) const
{
    TString query = Format("%v from [%v] where %v", columns, Path_, where);

    YT_LOG_DEBUG(
        "Invoking select query (Query: %Qv)",
        query);

    return Client_->SelectRows(query).Apply(BIND([&] (const TSelectRowsResult& result) {
        const auto& rowset = result.Rowset;
        return TRow::ParseRowRange(rowset->GetRows(), rowset->GetNameTable(), rowset->GetSchema());
    }));
}

////////////////////////////////////////////////////////////////////////////////

bool TQueueId::operator==(const TQueueId& other) const
{
    return Cluster == other.Cluster && Path == other.Path;
}

TString ToString(const TQueueId& queueId)
{
    return Format("%v:%v", queueId.Cluster, queueId.Path);
}

void Serialize(const TQueueId& queueId, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).Value(ToString(queueId));
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
    auto revisionId = nameTable->FindId("revision");
    auto dynamicId = nameTable->FindId("dynamic");
    auto sortedId = nameTable->FindId("sorted");

    for (const auto& row : rows) {
        auto& typedRow = typedRows.emplace_back();
        typedRow.QueueId = TQueueId{TString(row[*clusterId].AsStringBuf()), TString(row[*pathId].AsStringBuf())};

        auto findValue = [&] (std::optional<int> id) -> std::optional<TUnversionedValue> {
            if (id && row[*id].Type != EValueType::Null) {
                return row[*id];
            }
            return std::nullopt;
        };

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

void Serialize(const TQueueTableRow& row, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("queue_id").Value(row.QueueId)
            .Item("revision").Value(row.Revision)
            .Item("object_type").Value(row.ObjectType)
            .Item("dynamic").Value(row.Dynamic)
            .Item("sorted").Value(row.Sorted)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

template class TTableBase<TQueueTableRow, TQueueId>;

TQueueTable::TQueueTable(TYPath root, IClientPtr client)
    : TTableBase<TQueueTableRow, TQueueId>(root + "/" + TQueueTableDescriptor::Name, std::move(client))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent

size_t THash<NYT::NQueueAgent::TQueueId>::operator()(const NYT::NQueueAgent::TQueueId& queueId) const
{
    using NYT::HashCombine;

    size_t result = 0;
    HashCombine(result, queueId.Cluster);
    HashCombine(result, queueId.Path);
    return result;
}
