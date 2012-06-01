#include "table_executor.h"
#include "preprocess.h"

#include <ytlib/job_proxy/config.h>
#include <ytlib/driver/driver.h>

namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TReadExecutor::TReadExecutor()
    : PathArg("path", "path to a table in Cypress that must be read", true, "", "ypath")
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

Stroka TReadExecutor::GetDriverCommandName() const
{
    return "read";
}

//////////////////////////////////////////////////////////////////////////////////

TWriteExecutor::TWriteExecutor()
    : PathArg("path", "path to a table in Cypress that must be written", true, "", "ypath")
    , ValueArg("value", "row(s) to write", false, "", "yson")
    , SortedBy("", "sorted_by", "key columns names (table must initially be empty, input data must be sorted)", false, "", "list_fragment")
{
    CmdLine.add(PathArg);
    CmdLine.add(ValueArg);
    CmdLine.add(SortedBy);
}

void TWriteExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());
    auto value = ValueArg.getValue();
    // TODO(babenko): refactor
    auto sortedBy = DeserializeFromYson< yvector<Stroka> >("[" + SortedBy.getValue() + "]");

    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("write")
        .Item("path").Scalar(path)
        .DoIf(!sortedBy.empty(), [=] (TFluentMap fluent) {
            fluent.Item("sorted_by").List(sortedBy);
        })
        .DoIf(!value.empty(), [=] (TFluentMap fluent) {
                fluent.Item("value").Node(value);
        });

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TWriteExecutor::GetDriverCommandName() const
{
    return "write";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
