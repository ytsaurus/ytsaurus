#include "interop.h"
#include "type_builder.h"
#include "data_builder.h"

#include <yt/yt/library/formats/skiff_parser.h>

#include <yt/yt/library/skiff_ext/schema_match.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/api/native/client.h>

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
#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <library/cpp/iterator/functools.h>

#include <library/cpp/type_info/type_info.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yt/memory/ref.h>

#include <yql/essentials/public/result_format/yql_result_format_response.h>

namespace NYT::NYqlAgent {

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

static constexpr auto& Logger = YqlAgentLogger;

////////////////////////////////////////////////////////////////////////////////

struct TYqlRef
    : public TYsonStruct
{
    std::vector<TString> Reference;
    std::optional<std::vector<std::string>> Columns;

    REGISTER_YSON_STRUCT(TYqlRef);

    static void Register(TRegistrar registrar)
    {
        // Note that YQL does not follow our lowercase YSON field naming convention.
        registrar.Parameter("Reference", &TThis::Reference);
        registrar.Parameter("Columns", &TThis::Columns)
            .Default();
    }
};

DECLARE_REFCOUNTED_STRUCT(TYqlRef)
DEFINE_REFCOUNTED_TYPE(TYqlRef)

void ReorderAndSaveRows(
    TRowBufferPtr rowBuffer,
    TNameTablePtr sourceNameTable,
    TNameTablePtr targetNameTable,
    TRange<TUnversionedRow> rows,
    std::vector<TUnversionedRow>& resultRows)
{
    std::vector<int> sourceIdToTargetId;

    for (const auto row : rows) {
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
};

TYqlRowset BuildRowsetByRef(
    const TClientDirectoryPtr& clientDirectory,
    TYqlRefPtr references,
    int resultIndex,
    i64 rowCountLimit)
{
    Cerr << __func__ << ' ' << references->Reference.size() << ' ' << references->Reference.front() << ",...," << references->Reference.back() << ' ' << references->Columns->size() << Endl;

    if (references->Reference.size() != 3 || references->Reference[0] != "yt") {
        THROW_ERROR_EXCEPTION("Malformed YQL reference %v", references->Reference);
    }
    const auto& cluster = references->Reference[1];
    auto table = references->Reference[2];
    if (!table.StartsWith("#") && !table.StartsWith("//")) {
        // Best effort to deYQLize paths.
        table = "//" + table;
    }
    auto client = clientDirectory->GetClientOrThrow(cluster);

    TTableSchemaPtr targetSchema;
    TNameTablePtr targetNameTable;
    TNameTablePtr sourceNameTable;
    std::vector<TUnversionedRow> resultRows;
    auto rowBuffer = New<TRowBuffer>();

    auto isDynamicTable = ConvertTo<bool>(
        WaitFor(client->GetNode(table + "/@dynamic"))
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

        auto reader = WaitFor(client->CreateTableReader(path))
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
    };
}

TTableSchemaPtr BuildSchema(const TLogicalType& type) {
    std::vector<TColumnSchema> columns;
    for (const auto& member : type.AsListTypeRef().GetElement()->AsStructTypeRef().GetFields()) {
        columns.emplace_back(member.Name, member.Type);
    }
    return New<TTableSchema>(columns);
}

TYqlRowset BuildRowset(const TBuildingValueConsumer& consumer,THashMap<TString, ui32> columns, int resultIndex, bool incomplete)
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
            schema->GetStrict(),
            schema->GetUniqueKeys(),
            schema->GetSchemaModification(),
            schema->DeletedColumns());
    };
    auto targetSchema = reorderSchema(sourceSchema);
    const auto targetNameTable = TNameTable::FromSchema(*targetSchema);

    ReorderAndSaveRows(rowBuffer, sourceNameTable, targetNameTable, consumer.GetRows(), resultRows);

    YT_LOG_DEBUG("Result read (RowCount: %v, Incomplete: %v, ResultIndex: %v)", resultRows.size(), incomplete, resultIndex);

    std::cerr << __func__ << std::endl;
    PrintTo(*targetSchema, &std::cerr);

    Cerr << Endl << "rows: " << resultRows.size() << Endl;
    for (const auto& row : resultRows) {
        PrintTo(row, &std::cerr);
    }
    Cerr << Endl;

    return TYqlRowset{
        .TargetSchema = std::move(targetSchema),
        .ResultRows = std::move(resultRows),
        .RowBuffer = std::move(rowBuffer),
        .Incomplete = incomplete,
    };
}

TYqlRowset BuildRowsetFromYson(
    const TClientDirectoryPtr& clientDirectory,
    const NYql::NResult::TWrite& write,
    int resultIndex,
    i64 rowCountLimit)
{
    if (!write.Refs.empty()) {
        if (write.Refs.size() != 1) {
            THROW_ERROR_EXCEPTION("YQL returned non-singular ref, such response is not supported yet.");
        }
        auto ref = New<TYqlRef>();
        ref->Reference = write.Refs.front().Reference;
        if (const auto& columns = write.Refs.front().Columns) {
            ref->Columns.emplace(columns->size());
            std::copy(columns->cbegin(), columns->cend(), ref->Columns->begin());
        }
        return BuildRowsetByRef(clientDirectory, std::move(ref), resultIndex, rowCountLimit);
    }

    TTypeBuilder typeBuilder;
    NYql::NResult::ParseType(*write.Type, typeBuilder);
    const auto schema = BuildSchema(*typeBuilder.GetResult());
    TBuildingValueConsumer consumer(schema, YqlAgentLogger(), true);
    TDataBuilder dataBuilder(&consumer);
    NYql::NResult::ParseData(*write.Type, *write.Data, dataBuilder);

    THashMap<TString, ui32> columns;
/*  if (writeNode.HasKey("Columns")) {
        const auto& columnsNode = writeNode["Columns"];
        columns.reserve(columnsNode.Size());
        for (ui32 index = 0U; index < columnsNode.Size(); ++index) {
            columns[columnsNode.ChildAsString(index)] = index;
        }
    }
*/
    return BuildRowset(consumer, std::move(columns), resultIndex, write.IsTruncated);
}

TYqlRowset BuildRowsetFromSkiff(
    const TClientDirectoryPtr& clientDirectory,
    const INodePtr& resultNode,
    int resultIndex,
    i64 rowCountLimit)
{
    const auto& writeNode = resultNode->AsMap()->GetChildOrThrow("Write")->AsList()->GetChildOrThrow(0)->AsMap();
    if (writeNode->FindChild("Ref")) {
        const auto& refsNode = writeNode->GetChildOrThrow("Ref")->AsList();
        if (refsNode->GetChildCount() != 1) {
            THROW_ERROR_EXCEPTION("YQL returned non-singular ref, such response is not supported yet.");
        }
        return BuildRowsetByRef(clientDirectory, ConvertTo<TYqlRefPtr>(refsNode->GetChildOrThrow(0)), resultIndex, rowCountLimit);
    }

    const auto& skiffTypeNode = writeNode->GetChildOrThrow("SkiffType");
    auto config = ConvertTo<NFormats::TSkiffFormatConfigPtr>(&skiffTypeNode->Attributes());
    auto skiffSchemas = NSkiffExt::ParseSkiffSchemas(config->SkiffSchemaRegistry, config->TableSkiffSchemas);

    const auto& typeNode = writeNode->GetChildOrThrow("Type");
    auto schema = NYT::NYTree::ConvertTo<NYT::NTableClient::TTableSchemaPtr>(typeNode);
    TBuildingValueConsumer consumer(schema, YqlAgentLogger(), true);

    THashMap<TString, ui32> columns;
    if (const auto& columnsPtr = writeNode->FindChild("Columns")) {
        ui32 index = 0;
        for (const auto& column : ConvertTo<std::vector<TString>>(columnsPtr)) {
            columns[column] = index;
            index++;
        }
    }

    const auto data = writeNode->GetChildOrThrow("Data")->AsString()->GetValue();
    const auto parser = CreateParserForSkiff(&consumer, skiffSchemas, config, 0);
    parser->Read(data);
    parser->Finish();

    const auto incompleteNode = writeNode->FindChild("Incomplete");
    const bool incomplete = incompleteNode ? incompleteNode->AsBoolean()->GetValue() : false;

    return BuildRowset(consumer, std::move(columns), resultIndex, incomplete);
}

TWireYqlRowset MakeWireYqlRowset(const TYqlRowset& rowset)
{
    auto wireWriter = CreateWireProtocolWriter();
    wireWriter->WriteTableSchema(*rowset.TargetSchema);
    wireWriter->WriteSchemafulRowset(rowset.ResultRows);
    auto refs = wireWriter->Finish();

    struct TYqlRefMergeTag {};
    return {.WireRowset = MergeRefsToRef<TYqlRefMergeTag>(refs), .Incomplete = rowset.Incomplete};
}

std::vector<TWireYqlRowset> BuildRowsets(
    const TClientDirectoryPtr& clientDirectory,
    const TString& yqlYsonResults,
    i64 rowCountLimit)
{
    Cerr << __func__ << Endl << NYT::NodeToCanonicalYsonString(NYT::NodeFromYsonString(yqlYsonResults)) << Endl;
    std::vector<TWireYqlRowset> rowsets;
    try {
        const auto& responseNode = NYT::NodeFromYsonString(yqlYsonResults);
        const auto& response = NYql::NResult::ParseResponse(responseNode);
        rowsets.reserve(response.size());
        for (size_t index = 0U; index < response.size(); ++index) {
            YT_LOG_DEBUG("Building rowset for query result (ResultIndex: %v)", index);
            auto rowset = MakeWireYqlRowset(BuildRowsetFromYson(clientDirectory, response[index].Writes.front(), index, rowCountLimit));
            YT_LOG_DEBUG("Rowset built (ResultBytes: %v)", rowset.WireRowset.size());
            rowsets.push_back(std::move(rowset));
        }
        return rowsets;
    } catch (const NYql::NResult::TUnsupportedException& ex) {
        const auto error = TError(ex);
        YT_LOG_DEBUG("Error building rowset result from yson: %v. Try fallback to skiff.", error);
    } catch (const std::exception& ex) {
        const auto error = TError(ex);
        YT_LOG_DEBUG("Error building rowset result from yson: %v. Try fallback to skiff.", error);
    }

    // TODO: Remove the code below after switch on Yson in the plugin.
    const auto results = ConvertTo<std::vector<INodePtr>>(TYsonString(yqlYsonResults));
    for (const auto& [index, result] : Enumerate(results)) {
        try {
            YT_LOG_DEBUG("Building rowset for query result (ResultIndex: %v)", index);
            auto rowset = MakeWireYqlRowset(BuildRowsetFromSkiff(clientDirectory, result, index, rowCountLimit));
            YT_LOG_DEBUG("Rowset built (ResultBytes: %v)", rowset.WireRowset.size());
            rowsets.push_back(std::move(rowset));
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            YT_LOG_DEBUG("Error building rowset result (ResultIndex: %v, Error: %v)", index, error);
            rowsets.push_back({.Error = error});
        }
    }
    return rowsets;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
