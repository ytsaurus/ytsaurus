#include "table_executors.h"
#include "preprocess.h"

#include <ytlib/driver/driver.h>

#include <ytlib/table_client/unversioned_row.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NYson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TReadTableExecutor::TReadTableExecutor()
    : PathArg("path", "table path to read", true, "", "YPATH")
{
    CmdLine.add(PathArg);
}

void TReadTableExecutor::BuildParameters(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Value(path);

    TTransactedExecutor::BuildParameters(consumer);
}

Stroka TReadTableExecutor::GetCommandName() const
{
    return "read_table";
}

//////////////////////////////////////////////////////////////////////////////////

TWriteTableExecutor::TWriteTableExecutor()
    : PathArg("path", "table path to write", true, "", "YPATH")
    , ValueArg("value", "row(s) to write", false, "", "YSON")
    , SortedByArg("", "sorted_by", "key columns names (for sorted write)", false, "", "YSON_LIST_FRAGMENT")
    , UseStdIn(true)
{
    CmdLine.add(PathArg);
    CmdLine.add(ValueArg);
    CmdLine.add(SortedByArg);
}

void TWriteTableExecutor::BuildParameters(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());
    auto sortedBy = ConvertTo< std::vector<Stroka> >(TYsonString(SortedByArg.getValue(), EYsonType::ListFragment));

    const auto& value = ValueArg.getValue();
    if (!value.empty()) {
        Stream.Write(value);
        UseStdIn = false;
    }

    if (!sortedBy.empty()) {
        path.Attributes().Set("sorted_by", sortedBy);
    }

    BuildYsonMapFluently(consumer)
        .Item("path").Value(path);

    TTransactedExecutor::BuildParameters(consumer);
}

TInputStream* TWriteTableExecutor::GetInputStream()
{
    return UseStdIn ? &StdInStream() : &Stream;
}

Stroka TWriteTableExecutor::GetCommandName() const
{
    return "write_table";
}

////////////////////////////////////////////////////////////////////////////////

TTabletExecutor::TTabletExecutor()
    : PathArg("path", "table path", true, "", "YPATH")
    , FirstTabletIndexArg("", "first", "first tablet index", false, -1, "INTEGER")
    , LastTabletIndexArg("", "last", "last tablet index", false, -1, "INTEGER")
{
    CmdLine.add(PathArg);
    CmdLine.add(FirstTabletIndexArg);
    CmdLine.add(LastTabletIndexArg);
}

void TTabletExecutor::BuildParameters(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Value(path)
        .DoIf(FirstTabletIndexArg.isSet(), [&] (TFluentMap fluent) {
            fluent.Item("first_tablet_index").Value(FirstTabletIndexArg.getValue());
        })
        .DoIf(LastTabletIndexArg.isSet(), [&] (TFluentMap fluent) {
            fluent.Item("last_tablet_index").Value(LastTabletIndexArg.getValue());
        });
}

////////////////////////////////////////////////////////////////////////////////

TMountTableExecutor::TMountTableExecutor()
{ }

void TMountTableExecutor::BuildParameters(IYsonConsumer* consumer)
{
    TTabletExecutor::BuildParameters(consumer);
}

Stroka TMountTableExecutor::GetCommandName() const
{
    return "mount_table";
}

////////////////////////////////////////////////////////////////////////////////

TUnmountTableExecutor::TUnmountTableExecutor()
    : ForceArg("", "force", "unmount immediately, don't flush", false)
{
    CmdLine.add(ForceArg);
}

void TUnmountTableExecutor::BuildParameters(IYsonConsumer* consumer)
{
    TTabletExecutor::BuildParameters(consumer);

    BuildYsonMapFluently(consumer)
        .Item("force").Value(ForceArg.getValue());
}

Stroka TUnmountTableExecutor::GetCommandName() const
{
    return "unmount_table";
}

////////////////////////////////////////////////////////////////////////////////

TRemountTableExecutor::TRemountTableExecutor()
{ }

void TRemountTableExecutor::BuildParameters(IYsonConsumer* consumer)
{
    TTabletExecutor::BuildParameters(consumer);
}

Stroka TRemountTableExecutor::GetCommandName() const
{
    return "remount_table";
}

////////////////////////////////////////////////////////////////////////////////

TReshardTableExecutor::TReshardTableExecutor()
    : PivotKeysArg("pivot_keys", "pivot keys", false, "", "YSON_LIST_FRAGMENT")
{
    CmdLine.add(PivotKeysArg);
}

void TReshardTableExecutor::BuildParameters(IYsonConsumer* consumer)
{
    TTabletExecutor::BuildParameters(consumer);

    std::vector<TOwningKey> pivotKeys;
    for (const auto& key : PivotKeysArg) {
        pivotKeys.push_back(ConvertTo<TOwningKey>(TYsonString(key, EYsonType::ListFragment)));
    }

    BuildYsonMapFluently(consumer)
        .Item("pivot_keys").Value(pivotKeys);
}

Stroka TReshardTableExecutor::GetCommandName() const
{
    return "reshard_table";
}

////////////////////////////////////////////////////////////////////////////////

TInsertRowsExecutor::TInsertRowsExecutor()
    : PathArg("path", "table path to insert into", true, "", "YPATH")
    , UpdateArg("", "update", "update row values, don't replace the whole row", false)
    , ValueArg("value", "row(s) to write", false, "", "YSON")
{
    CmdLine.add(PathArg);
    CmdLine.add(UpdateArg);
    CmdLine.add(ValueArg);
}

void TInsertRowsExecutor::BuildParameters(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    const auto& value = ValueArg.getValue();
    if (!value.empty()) {
        Stream.Write(value);
        UseStdIn = false;
    }

    BuildYsonMapFluently(consumer)
        .Item("path").Value(path)
        .Item("update").Value(UpdateArg.getValue());

    TRequestExecutor::BuildParameters(consumer);
}

TInputStream* TInsertRowsExecutor::GetInputStream()
{
    return UseStdIn ? &StdInStream() : &Stream;
}

Stroka TInsertRowsExecutor::GetCommandName() const
{
    return "insert_rows";
}

////////////////////////////////////////////////////////////////////////////////

TSelectRowsExecutor::TSelectRowsExecutor()
    : QueryArg("query", "query to execute", true, "", "QUERY")
    , TimestampArg("", "timestamp", "timestamp to use", false, NTransactionClient::SyncLastCommittedTimestamp, "TIMESTAMP")
    , InputRowLimitArg("", "input_row_limit", "input rows limit", false, std::numeric_limits<int>::max(), "INTEGER")
    , OutputRowLimitArg("", "output_row_limit", "output rows limit", false, std::numeric_limits<int>::max(), "INTEGER")
    , RangeExpansionLimitArg("", "range_expansion_limit", "range expansion limit", false, 1000, "INTEGER")
    , VerboseLoggingArg("", "verbose_logging", "verbose logging", false)
    , EnableCodeCacheArg("", "enable_code_cache", "enable_code_cache", true)
    , MaxSubqueriesArg("", "max_subqueries", "max subqueries", false, 0, "INTEGER")
{
    CmdLine.add(QueryArg);
    CmdLine.add(TimestampArg);
    CmdLine.add(InputRowLimitArg);
    CmdLine.add(OutputRowLimitArg);
    CmdLine.add(RangeExpansionLimitArg);
    CmdLine.add(VerboseLoggingArg);
    CmdLine.add(EnableCodeCacheArg);
    CmdLine.add(MaxSubqueriesArg);
}

void TSelectRowsExecutor::BuildParameters(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("query").Value(QueryArg.getValue())
        .DoIf(TimestampArg.isSet(), [&] (TFluentMap fluent) {
            fluent.Item("timestamp").Value(TimestampArg.getValue());
        })
        .DoIf(InputRowLimitArg.isSet(), [&] (TFluentMap fluent) {
            fluent.Item("input_row_limit").Value(InputRowLimitArg.getValue());
        })
        .DoIf(OutputRowLimitArg.isSet(), [&] (TFluentMap fluent) {
            fluent.Item("output_row_limit").Value(OutputRowLimitArg.getValue());
        })
        .DoIf(RangeExpansionLimitArg.isSet(), [&] (TFluentMap fluent) {
            fluent.Item("range_expansion_limit").Value(RangeExpansionLimitArg.getValue());
        })
        .Item("verbose_logging").Value(VerboseLoggingArg.getValue())
        .Item("enable_code_cache").Value(EnableCodeCacheArg.getValue())
        .DoIf(MaxSubqueriesArg.isSet(), [&] (TFluentMap fluent) {
            fluent.Item("max_subqueries").Value(MaxSubqueriesArg.getValue());
        });
}

Stroka TSelectRowsExecutor::GetCommandName() const
{
    return "select_rows";
}

////////////////////////////////////////////////////////////////////////////////

TLookupRowsExecutor::TLookupRowsExecutor()
    : PathArg("path", "table path to lookup", true, "", "YPATH")
    , TimestampArg("", "timestamp", "timestamp to use", false, NTransactionClient::SyncLastCommittedTimestamp, "TIMESTAMP")
    , ValueArg("value", "keys(s) to lookup", false, "", "YSON")
{
    CmdLine.add(PathArg);
    CmdLine.add(TimestampArg);
    CmdLine.add(ValueArg);
}

void TLookupRowsExecutor::BuildParameters(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    const auto& value = ValueArg.getValue();
    if (!value.empty()) {
        Stream.Write(value);
        UseStdIn = false;
    }

    BuildYsonMapFluently(consumer)
        .Item("path").Value(path)
        .DoIf(TimestampArg.isSet(), [&] (TFluentMap fluent) {
            fluent.Item("timestamp").Value(TimestampArg.getValue());
        });
}

Stroka TLookupRowsExecutor::GetCommandName() const
{
    return "lookup_rows";
}

TInputStream* TLookupRowsExecutor::GetInputStream()
{
    return UseStdIn ? &StdInStream() : &Stream;
}

////////////////////////////////////////////////////////////////////////////////

TDeleteRowsExecutor::TDeleteRowsExecutor()
    : PathArg("path", "table path to delete rows from", true, "", "YPATH")
    , ValueArg("value", "keys(s) to delete", false, "", "YSON")
{
    CmdLine.add(PathArg);
    CmdLine.add(ValueArg);
}

void TDeleteRowsExecutor::BuildParameters(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    const auto& value = ValueArg.getValue();
    if (!value.empty()) {
        Stream.Write(value);
        UseStdIn = false;
    }

    BuildYsonMapFluently(consumer)
        .Item("path").Value(path);
}

Stroka TDeleteRowsExecutor::GetCommandName() const
{
    return "delete_rows";
}

TInputStream* TDeleteRowsExecutor::GetInputStream()
{
    return UseStdIn ? &StdInStream() : &Stream;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
