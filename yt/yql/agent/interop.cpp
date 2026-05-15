#include "interop.h"
#include "type_builder.h"
#include "data_builder.h"

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <yt/yt/client/api/table_reader.h>
#include <yt/yt/client/api/client.h>

#include <yt/yt/client/formats/parser.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/value_consumer.h>
#include <yt/yt/client/table_client/wire_protocol.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <library/cpp/iterator/functools.h>

#include <library/cpp/type_info/type_info.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/yt/memory/ref.h>

#include <yql/essentials/public/result_format/yql_result_format_response.h>

namespace NYT::NYqlAgent {

using namespace NApi;
using namespace NApi::NRpcProxy;
using namespace NYson;
using namespace NYTree;
using namespace NHiveClient;
using namespace NFuncTools;
using namespace NLogging;
using namespace NConcurrency;
using namespace NYPath;
using namespace NTableClient;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = YqlAgentLogger;

////////////////////////////////////////////////////////////////////////////////

bool ReorderAndSaveRows(
    TRowBufferPtr rowBuffer,
    TNameTablePtr sourceNameTable,
    TNameTablePtr targetNameTable,
    TRange<TUnversionedRow> rows,
    std::vector<TUnversionedRow>& resultRows,
    std::optional<i64> rowCountLimit)
{
    std::vector<int> sourceIdToTargetId;
    bool truncated = false;

    for (const auto row : rows) {
        if (rowCountLimit && resultRows.size() >= rowCountLimit) {
            truncated = true;
            break;
        }
        if (!row) {
            resultRows.push_back(row);
            continue;
        }

        auto reorderedRow = rowBuffer->AllocateUnversioned(targetNameTable->GetSize());
        for (int index = 0; index < static_cast<int>(reorderedRow.GetCount()); ++index) {
            reorderedRow[index] = MakeUnversionedSentinelValue(EValueType::Null, index);
        }
        for (const auto& value : row) {
            auto targetId = VectorAtOr(sourceIdToTargetId, value.Id, /*defaultValue*/ -1);
            if (targetId == -1) {
                auto name = sourceNameTable->GetName(value.Id);
                auto optionalTargetId = targetNameTable->FindId(name);
                if (!optionalTargetId) {
                    continue;
                }

                targetId = *optionalTargetId;
                AssignVectorAt(sourceIdToTargetId, value.Id, targetId, /*defaultValue*/ -1);
            }
            YT_VERIFY(0 <= targetId && targetId < targetNameTable->GetSize());
            reorderedRow[targetId] = rowBuffer->CaptureValue(value);
            reorderedRow[targetId].Id = targetId;
        }
        resultRows.push_back(reorderedRow);
    }

    return truncated;
};

void TYqlRef::Register(TRegistrar registrar)
{
    // Note that YQL does not follow our lowercase YSON field naming convention.
    registrar.Parameter("Reference", &TThis::Reference);
    registrar.Parameter("Columns", &TThis::Columns)
        .Default();
}

TYqlRowset BuildRowsetByRef(
    const std::vector<std::pair<TString, TString>>& clusters,
    const TClientOptions& clientOptions,
    TYqlRefPtr references,
    int resultIndex,
    i64 rowCountLimit)
{
    if (references->Reference.size() != 3 || references->Reference[0] != "yt") {
        THROW_ERROR_EXCEPTION("Malformed YQL reference %v", references->Reference);
    }
    const auto& cluster = references->Reference[1];
    auto table = references->Reference[2];
    if (!table.StartsWith("#") && !table.StartsWith("//")) {
        // Best effort to deYQLize paths.
        table = "//" + table;
    }

    std::optional<TString> clusterAddress;
    for (const auto& clusterMapping : clusters) {
        if (clusterMapping.first == cluster) {
            clusterAddress = clusterMapping.second;
        }
    }
    if (!clusterAddress) {
        THROW_ERROR_EXCEPTION("Cluster %Qv address is not specified", cluster);
    }
    auto config = NRpcProxy::TConnectionConfig::CreateFromClusterUrl(*clusterAddress);
    auto connection = NRpcProxy::CreateConnection(config);
    auto client = connection->CreateClient(clientOptions);

    TTableSchemaPtr targetSchema;
    TNameTablePtr targetNameTable;
    TNameTablePtr sourceNameTable;
    std::vector<TUnversionedRow> resultRows;
    auto rowBuffer = New<TRowBuffer>();

    NApi::TGetNodeOptions options;
    options.Timeout = config->RpcTimeout;
    auto isDynamicTable = ConvertTo<bool>(
        WaitFor(client->GetNode(table + "/@dynamic", options))
            .ValueOrThrow());

    if (isDynamicTable) {
        YT_LOG_DEBUG("Selecting dynamic table rows (Table: %v)", table);

        auto selectResult = WaitFor(client->SelectRows(Format("* from [%v] limit %v", table, rowCountLimit + 1)))
            .ValueOrThrow();
        targetSchema = selectResult.Rowset->GetSchema()->Filter(references->Columns);
        targetNameTable = TNameTable::FromSchema(*targetSchema);
        sourceNameTable = selectResult.Rowset->GetNameTable();

        YT_LOG_DEBUG("Reading and reordering rows (TargetSchema: %v)", targetSchema);

        ReorderAndSaveRows(rowBuffer, sourceNameTable, targetNameTable, selectResult.Rowset->GetRows(), resultRows);
    } else {
        TRichYPath path(table);
        if (references->Columns) {
            path.SetColumns(*references->Columns);
        }
        TReadLimit upperReadLimit;
        upperReadLimit.SetRowIndex(rowCountLimit + 1);
        path.SetRanges({TReadRange({}, upperReadLimit)});

        YT_LOG_DEBUG("Opening static table reader (Path: %v)", path);

        // We must read 'Any' values as is, without change their type on 'String' or other to write the 'rowset' field correctly.
        auto reader = WaitFor(client->CreateTableReader(path, NApi::TTableReaderOptions{.EnableAnyUnpacking = false}))
            .ValueOrThrow();

        targetSchema = reader->GetTableSchema()->Filter(references->Columns);
        targetNameTable = TNameTable::FromSchema(*targetSchema);
        sourceNameTable = reader->GetNameTable();

        YT_LOG_DEBUG("Reading and reordering rows (TargetSchema: %v)", targetSchema);

        while (auto batch = reader->Read()) {
            if (batch->IsEmpty()) {
                WaitFor(reader->GetReadyEvent())
                    .ThrowOnError();
            }
            ReorderAndSaveRows(rowBuffer, sourceNameTable, targetNameTable, batch->MaterializeRows(), resultRows);
        }
    }

    bool incomplete = false;
    if (std::ssize(resultRows) > rowCountLimit) {
        resultRows.resize(rowCountLimit);
        incomplete = true;
    }
    YT_LOG_DEBUG("Result read (RowCount: %v, Incomplete: %v, ResultIndex: %v)", resultRows.size(), incomplete, resultIndex);

    return TYqlRowset {
        .TargetSchema = targetSchema,
        .ResultRows = resultRows,
        .RowBuffer = rowBuffer,
        .Incomplete = incomplete,
        .References = references,
    };
}

TTableSchemaPtr BuildSchema(const TLogicalType& type)
{
    std::vector<TColumnSchema> columns;

    if (type.GetMetatype() != ELogicalMetatype::List || type.AsListTypeRef().GetElement()->GetMetatype() != ELogicalMetatype::Struct) {
        THROW_ERROR_EXCEPTION("Non-table results are not supported in Query Tracker. Expected List<Struct<…>> but found %Qv",
            ToString(type));
    }

    auto rowType = type.AsListTypeRef().GetElement();
    for (const auto& member : rowType->AsStructTypeRef().GetFields()) {
        columns.emplace_back(member.Name, member.Type);
    }
    return New<TTableSchema>(columns);
}

TYqlRowset BuildRowset(
    const TBuildingValueConsumer& consumer,
    int resultIndex,
    bool incomplete,
    THashMap<TString, ui32> columns = {},
    std::optional<i64> rowCountLimit = std::nullopt)
{
    std::vector<TUnversionedRow> resultRows;
    const auto sourceSchema = consumer.GetSchema();
    const auto sourceNameTable = consumer.GetNameTable();
    auto rowBuffer = New<TRowBuffer>();

    const auto reorderSchema = [&] (TTableSchemaPtr schema) {
        if (columns.empty()) {
            return schema;
        }

        auto schemaColumns = schema->Columns();
        std::vector<TColumnSchema> reorderedColumns(columns.size());
        for (auto& column : schemaColumns) {
            reorderedColumns[columns[column.Name()]] = column;
        }

        return New<TTableSchema>(
            std::move(reorderedColumns),
            schema->IsStrict(),
            schema->IsUniqueKeys(),
            schema->GetSchemaModification(),
            schema->DeletedColumns());
    };
    auto targetSchema = reorderSchema(sourceSchema);
    const auto targetNameTable = TNameTable::FromSchema(*targetSchema);

    incomplete |= ReorderAndSaveRows(rowBuffer, sourceNameTable, targetNameTable, consumer.GetRows(), resultRows, rowCountLimit);

    YT_LOG_DEBUG("Result read (RowCount: %v, Incomplete: %v, ResultIndex: %v)", resultRows.size(), incomplete, resultIndex);

    return TYqlRowset{
        .TargetSchema = std::move(targetSchema),
        .ResultRows = std::move(resultRows),
        .RowBuffer = std::move(rowBuffer),
        .Incomplete = incomplete,
    };
}

TYqlRowset BuildRowsetFromYson(
    const std::vector<std::pair<TString, TString>>& clusters,
    const TClientOptions& clientOptions,
    const NYql::NResult::TWrite& write,
    int resultIndex,
    i64 rowCountLimit)
{
    if (!write.Refs.empty()) {
        if (write.Refs.size() != 1) {
            THROW_ERROR_EXCEPTION("YQL returned non-singular ref, such response is not supported yet");
        }
        auto ref = New<TYqlRef>();
        ref->Reference = write.Refs.front().Reference;
        if (const auto& columns = write.Refs.front().Columns) {
            ref->Columns.emplace(columns->size());
            std::copy(columns->cbegin(), columns->cend(), ref->Columns->begin());
        }
        return BuildRowsetByRef(clusters, clientOptions, std::move(ref), resultIndex, rowCountLimit);
    }

    TTypeBuilder typeBuilder;
    NYql::NResult::ParseType(*write.Type, typeBuilder);
    auto typeResult = typeBuilder.PullResult();
    const auto schema = BuildSchema(*typeResult);
    TBuildingValueConsumer consumer(schema, YqlAgentLogger(), true);
    TDataBuilder dataBuilder(&consumer, typeResult);
    NYql::NResult::ParseData(*write.Type, *write.Data, dataBuilder);
    return BuildRowset(consumer, resultIndex, write.IsTruncated, {}, rowCountLimit);
}

TWireYqlRowset MakeWireYqlRowset(const TYqlRowset& rowset)
{
    auto wireWriter = CreateWireProtocolWriter();
    wireWriter->WriteTableSchema(*rowset.TargetSchema);
    wireWriter->WriteSchemafulRowset(rowset.ResultRows);
    auto refs = wireWriter->Finish();

    struct TYqlRefMergeTag {};
    return {
        .WireRowset = MergeRefsToRef<TYqlRefMergeTag>(refs),
        .Incomplete = rowset.Incomplete,
        .References = rowset.References,
    };
}

std::vector<TWireYqlRowset> BuildRowsets(
    const std::vector<std::pair<TString, TString>>& clusters,
    const TClientOptions& clientOptions,
    const TString& yqlYsonResults,
    i64 rowCountLimit)
{
    std::vector<TWireYqlRowset> rowsets;
    try {
        const auto& responseNode = NYT::NodeFromYsonString(yqlYsonResults);
        const auto& response = NYql::NResult::ParseResponse(responseNode);
        rowsets.reserve(response.size());
        for (size_t index = 0U; index < response.size(); ++index) {
            YT_LOG_DEBUG("Building rowset for query result (ResultIndex: %v)", index);
            auto rowset = MakeWireYqlRowset(BuildRowsetFromYson(clusters, clientOptions, response[index].Writes.front(), index, rowCountLimit));
            YT_LOG_DEBUG("Rowset built (ResultBytes: %v)", rowset.WireRowset.size());
            rowsets.push_back(std::move(rowset));
        }
    } catch (const std::exception& ex) {
        auto error = TError(ex);
        YT_LOG_DEBUG(error, "Error building rowset result.");
        rowsets.push_back({.Error = error});
    }
    return rowsets;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
