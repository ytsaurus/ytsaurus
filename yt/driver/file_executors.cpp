#include "file_executors.h"
#include "preprocess.h"

#include <ytlib/driver/driver.h>

#include <server/job_proxy/config.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TWriteFileExecutor::TWriteFileExecutor()
    : PathArg("path", "file path to create", true, TRichYPath(""), "YPATH")
{
    CmdLine.add(PathArg);
}

void TWriteFileExecutor::BuildParameters(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Value(path);

    TTransactedExecutor::BuildParameters(consumer);
}

Stroka TWriteFileExecutor::GetCommandName() const
{
    return "write_file";
}

//////////////////////////////////////////////////////////////////////////////////

TReadFileExecutor::TReadFileExecutor()
    : PathArg("path", "file path to download", true, TRichYPath(""), "YPATH")
{
    CmdLine.add(PathArg);
}

void TReadFileExecutor::BuildParameters(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Value(path);

    TTransactedExecutor::BuildParameters(consumer);
}

Stroka TReadFileExecutor::GetCommandName() const
{
    return "read_file";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
