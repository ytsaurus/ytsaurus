#include "file_executors.h"
#include "preprocess.h"

#include <ytlib/driver/driver.h>

#include <server/job_proxy/config.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TUploadExecutor::TUploadExecutor()
    : PathArg("path", "file path to create", true, TRichYPath(""), "YPATH")
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
    : PathArg("path", "file path to download", true, TRichYPath(""), "YPATH")
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

} // namespace NDriver
} // namespace NYT
