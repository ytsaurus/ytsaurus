#include "file_executors.h"
#include "preprocess.h"

#include <ytlib/job_proxy/config.h>
#include <ytlib/driver/driver.h>

namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TUploadExecutor::TUploadExecutor()
    : PathArg("path", "to a new file in Cypress that must be uploaded", true, "", "YPATH")
{
    CmdLine.add(PathArg);
}

void TUploadExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path);

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TUploadExecutor::GetCommandName() const
{
    return "upload";
}

//////////////////////////////////////////////////////////////////////////////////

TDownloadExecutor::TDownloadExecutor()
    : PathArg("path", "path to a file in Cypress that must be downloaded", true, "", "YPATH")
{
    CmdLine.add(PathArg);
}

void TDownloadExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path);

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TDownloadExecutor::GetCommandName() const
{
    return "download";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
