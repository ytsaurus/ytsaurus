#include "interop.h"

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/api/table_reader.h>
#include <yt/yt/client/api/client.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/row_batch.h>
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

TYqlRowset BuildRowset(
    const TClientDirectoryPtr& clientDirectory,
    const INodePtr& resultNode,
    int resultIndex,
    i64 rowCountLimit)
{
    YT_LOG_DEBUG("Result node (ResultNode: %v, ResultIndex: %v)", ConvertToYsonString(resultNode, EYsonFormat::Text).ToString(), resultIndex);
    const auto& mapNode = resultNode->AsMap();
    const auto& refsNode = mapNode->GetChildOrThrow("Write")->AsList()->GetChildOrThrow(0)->AsMap()->GetChildOrThrow("Ref")->AsList();
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
    std::vector<int> sourceIdToTargetId;
    auto rowBuffer = New<TRowBuffer>();

    auto reorderAndSaveRows = [&] (TRange<TUnversionedRow> rows) {
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

        reorderAndSaveRows(selectResult.Rowset->GetRows());
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
            reorderAndSaveRows(batch->MaterializeRows());
        }
    }

    bool incomplete = false;
    if (std::ssize(resultRows) > rowCountLimit) {
        resultRows.resize(rowCountLimit);
        incomplete = true;
    }
    YT_LOG_DEBUG("Result read (RowCount: %v, Incomplete: %v, ResultIndex: %v)", resultRows.size(), incomplete, resultIndex);
    auto wireWriter = CreateWireProtocolWriter();
    wireWriter->WriteTableSchema(*targetSchema);
    wireWriter->WriteSchemafulRowset(resultRows);
    auto refs = wireWriter->Finish();
    struct TYqlRefMergeTag {};
    return {.WireRowset = MergeRefsToRef<TYqlRefMergeTag>(refs), .Incomplete = incomplete};
}

std::vector<TYqlRowset> BuildRowsets(
    const TClientDirectoryPtr& clientDirectory,
    const TString& yqlYsonResults,
    i64 rowCountLimit)
{
    auto results = ConvertTo<std::vector<INodePtr>>(TYsonString(yqlYsonResults));
    std::vector<TYqlRowset> rowsets;
    for (const auto& [index, result] : Enumerate(results)) {
        try {
            YT_LOG_DEBUG("Building rowset for query result (ResultIndex: %v)", index);
            auto rowset = BuildRowset(clientDirectory, result, index, rowCountLimit);
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
