#include "interop.h"

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

#include <library/cpp/yt/memory/ref.h>

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

////////////////////////////////////////////////////////////////////////////

static const auto& Logger = YqlAgentLogger;

////////////////////////////////////////////////////////////////////////////

struct TYqlRef
    : public TYsonStruct
{
    std::vector<TString> Reference;
    std::optional<std::vector<TString>> Columns;

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

    for (const auto& row : rows) {
        if (!row) {
            resultRows.push_back(row);
            continue;
        }

        std::vector<TUnversionedValue> values(targetNameTable->GetSize());
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
    const IMapNodePtr& writeNode,
    int resultIndex,
    i64 rowCountLimit)
{
    const auto& refsNode = writeNode->GetChildOrThrow("Ref")->AsList();
    if (refsNode->GetChildCount() != 1) {
        THROW_ERROR_EXCEPTION("YQL returned non-singular ref, such response is not supported yet ");
    }
    const auto& references = ConvertTo<TYqlRefPtr>(refsNode->GetChildOrThrow(0));
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

TYqlRowset BuildRowset(
    const TClientDirectoryPtr& clientDirectory,
    const INodePtr& resultNode,
    int resultIndex,
    i64 rowCountLimit)
{
    const auto& mapNode = resultNode->AsMap();
    const auto& writeNode = mapNode->GetChildOrThrow("Write")->AsList()->GetChildOrThrow(0)->AsMap();

    if (writeNode->FindChild("Ref")) {
        return BuildRowsetByRef(clientDirectory, writeNode, resultIndex, rowCountLimit);
    }

    const auto& skiffTypeNode = writeNode->GetChildOrThrow("SkiffType");
    auto config = ConvertTo<NFormats::TSkiffFormatConfigPtr>(&skiffTypeNode->Attributes());
    auto skiffSchemas = NSkiffExt::ParseSkiffSchemas(config->SkiffSchemaRegistry, config->TableSkiffSchemas);

    const auto& typeNode = writeNode->GetChildOrThrow("Type");
    auto schema = NYT::NYTree::ConvertTo<NYT::NTableClient::TTableSchemaPtr>(typeNode);
    TBuildingValueConsumer consumer(schema, YqlAgentLogger, true);

    const auto& columnsPtr = writeNode->FindChild("Columns");
    THashMap<TString, ui32> columns;
    if (columnsPtr) {
        ui32 index = 0;
        for (const auto& column : ConvertTo<std::vector<TString>>(columnsPtr)) {
            columns[column] = index;
            index++;
        }
    }

    auto data = writeNode->GetChildOrThrow("Data")->AsString()->GetValue();
    auto parser = CreateParserForSkiff(&consumer, skiffSchemas, config, 0);
    parser->Read(data);
    parser->Finish();

    std::vector<TUnversionedRow> resultRows;
    auto sourceSchema = consumer.GetSchema();
    auto sourceNameTable = consumer.GetNameTable();
    auto rowBuffer = New<TRowBuffer>();

    auto reorderSchema = [&] (TTableSchemaPtr schema) {
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
    auto targetNameTable = TNameTable::FromSchema(*targetSchema);

    ReorderAndSaveRows(rowBuffer, sourceNameTable, targetNameTable, consumer.GetRows(), resultRows);

    auto incompletePtr = writeNode->FindChild("Incomplete");
    bool incomplete = incompletePtr ? incompletePtr->AsBoolean()->GetValue() : false;

    YT_LOG_DEBUG("Result read (RowCount: %v, Incomplete: %v, ResultIndex: %v)", resultRows.size(), incomplete, resultIndex);

    return TYqlRowset {
        .TargetSchema = targetSchema,
        .ResultRows = resultRows,
        .RowBuffer = rowBuffer,
        .Incomplete = incomplete,
    };
}


TWireYqlRowset MakeWireYqlRowset(const TYqlRowset& rowset) {
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
    auto results = ConvertTo<std::vector<INodePtr>>(TYsonString(yqlYsonResults));
    std::vector<TWireYqlRowset> rowsets;
    for (const auto& [index, result] : Enumerate(results)) {
        try {
            YT_LOG_DEBUG("Building rowset for query result (ResultIndex: %v)", index);
            auto rowset = MakeWireYqlRowset(BuildRowset(clientDirectory, result, index, rowCountLimit));
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


////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
