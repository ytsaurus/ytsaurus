#include "table_executors.h"
#include "preprocess.h"

#include <ytlib/driver/driver.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TReadExecutor::TReadExecutor()
    : PathArg("path", "table path to read", true, "", "YPATH")
{
    CmdLine.add(PathArg);
}

void TReadExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Value(path);

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TReadExecutor::GetCommandName() const
{
    return "read";
}

//////////////////////////////////////////////////////////////////////////////////

TWriteExecutor::TWriteExecutor()
    : PathArg("path", "table path to write", true, "", "YPATH")
    , ValueArg("value", "row(s) to write", false, "", "YSON")
    , SortedByArg("", "sorted_by", "key columns names (for sorted write)", false, "", "YSON_LIST_FRAGMENT")
    , UseStdIn(true)
{
    CmdLine.add(PathArg);
    CmdLine.add(ValueArg);
    CmdLine.add(SortedByArg);
}

void TWriteExecutor::BuildArgs(IYsonConsumer* consumer)
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

TInputStream* TWriteExecutor::GetInputStream()
{
    return UseStdIn ? &StdInStream() : &Stream;
}

Stroka TWriteExecutor::GetCommandName() const
{
    return "write";
}

////////////////////////////////////////////////////////////////////////////////

TMountExecutor::TMountExecutor()
    : PathArg("path", "table path to mount", true, "", "YPATH")
{
    CmdLine.add(PathArg);
}

void TMountExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Value(path);
}

Stroka TMountExecutor::GetCommandName() const 
{
    return "mount";
}

////////////////////////////////////////////////////////////////////////////////

TUnmountExecutor::TUnmountExecutor()
    : PathArg("path", "table path to unmount", true, "", "YPATH")
{
    CmdLine.add(PathArg);
}

void TUnmountExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Value(path);
}

Stroka TUnmountExecutor::GetCommandName() const 
{
    return "unmount";
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
{
    CmdLine.add(QueryArg);
}

void TSelectExecutor::BuildArgs(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("query").Value(QueryArg.getValue());
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
