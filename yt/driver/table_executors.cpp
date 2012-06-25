#include "table_executors.h"
#include "preprocess.h"

#include <ytlib/job_proxy/config.h>
#include <ytlib/driver/driver.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TReadExecutor::TReadExecutor()
    : PathArg("path", "path to a table in Cypress that must be read", true, "", "YPATH")
{
    CmdLine.add(PathArg);
}

void TReadExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("read")
        .Item("path").Scalar(path);

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TReadExecutor::GetCommandName() const
{
    return "read";
}

//////////////////////////////////////////////////////////////////////////////////

TWriteExecutor::TWriteExecutor()
    : PathArg("path", "path to a table in Cypress that must be written", true, "", "YPATH")
    , ValueArg("value", "row(s) to write", false, "", "YSON")
    , SortedBy("", "sorted_by", "key columns names (table must initially be empty, input data must be sorted)", false, "", "YSON_LIST_FRAGMENT")
    , UseStdIn(true)
{
    CmdLine.add(PathArg);
    CmdLine.add(ValueArg);
    CmdLine.add(SortedBy);
}

void TWriteExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());
    // TODO(babenko): refactor
    auto sortedBy = DeserializeFromYson< yvector<Stroka> >("[" + SortedBy.getValue() + "]");

    const auto& value = ValueArg.getValue();
    if (!value.empty()) {
        Stream.Write(value);
        UseStdIn = false;
    }

    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("write")
        .Item("path").Scalar(path)
        .DoIf(!sortedBy.empty(), [=] (TFluentMap fluent) {
            fluent.Item("sorted_by").List(sortedBy);
        });

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

} // namespace NDriver
} // namespace NYT
