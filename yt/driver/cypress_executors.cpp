#include "cypress_executors.h"
#include "preprocess.h"

#include <ytlib/job_proxy/config.h>
#include <ytlib/driver/driver.h>

namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TGetExecutor::TGetExecutor()
    : PathArg("path", "path to an object in Cypress that must be retrieved", true, "", "ypath")
{
    CmdLine.add(PathArg);
}

void TGetExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path);

    TTransactedExecutor::BuildArgs(consumer);
    BuildOptions(consumer);
}

Stroka TGetExecutor::GetCommandName() const
{
    return "get";
}

////////////////////////////////////////////////////////////////////////////////

TSetExecutor::TSetExecutor()
    : PathArg("path", "path to an object in Cypress that must be set", true, "", "ypath")
    , ValueArg("value", "value to set", false, "", "yson")
    , UseStdIn(true)
{
    CmdLine.add(PathArg);
    CmdLine.add(ValueArg);
}

void TSetExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    const auto& value = ValueArg.getValue();
    if (!value.empty()) {
        Stream.Write(value);
        UseStdIn = false;
    }

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path);

    TTransactedExecutor::BuildArgs(consumer);
    BuildOptions(consumer);
}

TInputStream* TSetExecutor::GetInputStream()
{
    if (UseStdIn) {
        return &StdInStream();
    } else {
        return &Stream;
    }
}

Stroka TSetExecutor::GetCommandName() const
{
    return "set";
}


////////////////////////////////////////////////////////////////////////////////

TRemoveExecutor::TRemoveExecutor()
    : PathArg("path", "path to an object in Cypress that must be removed", true, "", "ypath")
{
    CmdLine.add(PathArg);
}

void TRemoveExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path);

    TTransactedExecutor::BuildArgs(consumer);
    BuildOptions(consumer);
}

Stroka TRemoveExecutor::GetCommandName() const
{
    return "remove";
}

////////////////////////////////////////////////////////////////////////////////

TListExecutor::TListExecutor()
    : PathArg("path", "path to a object in Cypress whose children must be listed", true, "", "ypath")
{
    CmdLine.add(PathArg);
}

void TListExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path);

    TTransactedExecutor::BuildArgs(consumer);
    BuildOptions(consumer);
}

Stroka TListExecutor::GetCommandName() const
{
    return "list";
}

////////////////////////////////////////////////////////////////////////////////

TCreateExecutor::TCreateExecutor()
    : TypeArg("type", "type of node", true, NObjectServer::EObjectType::Null, "object type")
    , PathArg("path", "path for a new object in Cypress", true, "", "ypath")
{
    CmdLine.add(TypeArg);
    CmdLine.add(PathArg);
}

void TCreateExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path)
        .Item("type").Scalar(TypeArg.getValue().ToString());

    TTransactedExecutor::BuildArgs(consumer);
    BuildOptions(consumer);
}

Stroka TCreateExecutor::GetCommandName() const
{
    return "create";
}

////////////////////////////////////////////////////////////////////////////////

TLockExecutor::TLockExecutor()
    : TTransactedExecutor(true)
    , PathArg("path", "path to an object in Cypress that must be locked", true, "", "ypath")
    , ModeArg("", "mode", "lock mode", false, NCypress::ELockMode::Exclusive, "snapshot, shared, exclusive")
{
    CmdLine.add(PathArg);
    CmdLine.add(ModeArg);
}

void TLockExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path)
        .Item("mode").Scalar(ModeArg.getValue().ToString());

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TLockExecutor::GetCommandName() const
{
    return "lock";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
