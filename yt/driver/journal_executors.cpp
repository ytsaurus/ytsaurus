#include "journal_executors.h"
#include "preprocess.h"

#include <ytlib/driver/driver.h>

#include <ytlib/new_table_client/unversioned_row.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NYson;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

TReadJournalExecutor::TReadJournalExecutor()
    : PathArg("path", "journal path to read", true, "", "YPATH")
{
    CmdLine.add(PathArg);
}

void TReadJournalExecutor::BuildParameters(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Value(path);

    TTransactedExecutor::BuildParameters(consumer);
}

Stroka TReadJournalExecutor::GetCommandName() const
{
    return "read_journal";
}

//////////////////////////////////////////////////////////////////////////////////

TWriteJournalExecutor::TWriteJournalExecutor()
    : PathArg("path", "journal path to write", true, "", "YPATH")
    , ValueArg("value", "row(s) to write", false, "", "YSON")
    , UseStdIn(true)
{
    CmdLine.add(PathArg);
    CmdLine.add(ValueArg);
}

void TWriteJournalExecutor::BuildParameters(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    const auto& value = ValueArg.getValue();
    if (!value.empty()) {
        Stream.Write(value);
        UseStdIn = false;
    }

    BuildYsonMapFluently(consumer)
        .Item("path").Value(path);

    TTransactedExecutor::BuildParameters(consumer);
}

TInputStream* TWriteJournalExecutor::GetInputStream()
{
    return UseStdIn ? &StdInStream() : &Stream;
}

Stroka TWriteJournalExecutor::GetCommandName() const
{
    return "write_journal";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
