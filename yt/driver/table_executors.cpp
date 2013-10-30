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
    , SortedBy("", "sorted_by", "key columns names (for sorted write)", false, "", "YSON_LIST_FRAGMENT")
    , UseStdIn(true)
{
    CmdLine.add(PathArg);
    CmdLine.add(ValueArg);
    CmdLine.add(SortedBy);
}

void TWriteExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());
    auto sortedBy = ConvertTo< std::vector<Stroka> >(TYsonString(SortedBy.getValue(), EYsonType::ListFragment));

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

} // namespace NDriver
} // namespace NYT
