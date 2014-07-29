#include "table_executors.h"
#include "preprocess.h"

#include <ytlib/driver/driver.h>

#include <ytlib/new_table_client/unversioned_row.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NYson;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

TReadTableExecutor::TReadTableExecutor()
    : PathArg("path", "table path to read", true, "", "YPATH")
{
    CmdLine.add(PathArg);
}

void TReadTableExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Value(path);

    TTransactedExecutor::BuildArgs(consumer);
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

void TWriteTableExecutor::BuildArgs(IYsonConsumer* consumer)
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

    TTransactedExecutor::BuildArgs(consumer);
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

void TTabletExecutor::BuildArgs(IYsonConsumer* consumer)
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

void TMountTableExecutor::BuildArgs(IYsonConsumer* consumer)
{
    TTabletExecutor::BuildArgs(consumer);
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

void TUnmountTableExecutor::BuildArgs(IYsonConsumer* consumer)
{
    TTabletExecutor::BuildArgs(consumer);

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

void TRemountTableExecutor::BuildArgs(IYsonConsumer* consumer)
{
    TTabletExecutor::BuildArgs(consumer);
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

void TReshardTableExecutor::BuildArgs(IYsonConsumer* consumer)
{
    TTabletExecutor::BuildArgs(consumer);

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

TInsertExecutor::TInsertExecutor()
    : PathArg("path", "table path to insert into", true, "", "YPATH")
    , UpdateArg("", "update", "update row values, don't replace the whole row", false)
    , ValueArg("value", "row(s) to write", false, "", "YSON")
    , UseStdIn(true)
{
    CmdLine.add(PathArg);
    CmdLine.add(UpdateArg);
    CmdLine.add(ValueArg);
}

void TInsertExecutor::BuildArgs(IYsonConsumer* consumer)
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

    TRequestExecutor::BuildArgs(consumer);
}

TInputStream* TInsertExecutor::GetInputStream()
{
    return UseStdIn ? &StdInStream() : &Stream;
}

Stroka TInsertExecutor::GetCommandName() const
{
    return "insert";
}

////////////////////////////////////////////////////////////////////////////////

TSelectExecutor::TSelectExecutor()
    : QueryArg("query", "query to execute", true, "", "QUERY")
    , TimestampArg("", "timestamp", "timestamp to use", false, NTransactionClient::LastCommittedTimestamp, "TIMESTAMP")
    , InputRowLimitArg("", "input_row_limit", "input rows limit", false, std::numeric_limits<int>::max(), "INTEGER")
    , OutputRowLimitArg("", "output_row_limit", "output rows limit", false, std::numeric_limits<int>::max(), "INTEGER")
{
    CmdLine.add(QueryArg);
    CmdLine.add(TimestampArg);
    CmdLine.add(InputRowLimitArg);
    CmdLine.add(OutputRowLimitArg);
}

void TSelectExecutor::BuildArgs(IYsonConsumer* consumer)
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
        });
}

Stroka TSelectExecutor::GetCommandName() const 
{
    return "select";
}

////////////////////////////////////////////////////////////////////////////////

TLookupExecutor::TLookupExecutor()
    : PathArg("path", "table path to lookup", true, "", "YPATH")
    , KeyArg("key", "key to lookup", true, "", "YSON_LIST_FRAGMENT")
    , TimestampArg("", "timestamp", "timestamp to use", false, NTransactionClient::LastCommittedTimestamp, "TIMESTAMP")
{
    CmdLine.add(PathArg);
    CmdLine.add(KeyArg);
    CmdLine.add(TimestampArg);
}

void TLookupExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());
    auto key = ConvertTo<std::vector<INodePtr>>(TYsonString(KeyArg.getValue(), EYsonType::ListFragment));

    BuildYsonMapFluently(consumer)
        .Item("path").Value(path)
        .Item("key").Value(key)
        .DoIf(TimestampArg.isSet(), [&] (TFluentMap fluent) {
            fluent.Item("timestamp").Value(TimestampArg.getValue());
        });
}

Stroka TLookupExecutor::GetCommandName() const
{
    return "lookup";
}

////////////////////////////////////////////////////////////////////////////////

TDeleteExecutor::TDeleteExecutor()
    : PathArg("path", "table path to delete rows from", true, "", "YPATH")
    , KeyArg("key", "key to delete", true, "", "YSON_LIST_FRAGMENT")
{
    CmdLine.add(PathArg);
    CmdLine.add(KeyArg);
}

void TDeleteExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());
    auto key = ConvertTo<std::vector<INodePtr>>(TYsonString(KeyArg.getValue(), EYsonType::ListFragment));

    BuildYsonMapFluently(consumer)
        .Item("path").Value(path)
        .Item("key").Value(key);
}

Stroka TDeleteExecutor::GetCommandName() const
{
    return "delete";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
