#include "cypress_executors.h"
#include "preprocess.h"

#include <ytlib/driver/driver.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TGetExecutor::TGetExecutor()
    : PathArg("path", "path to an object in Cypress that must be retrieved", true, "", "YPATH")
    , AttributeArg("", "attribute", "attribute to fetch with node", false, "ATTRIBUTE")
{
    CmdLine.add(PathArg);
    CmdLine.add(AttributeArg);
}

void TGetExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path)
        .Item("attributes").List(AttributeArg);

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TGetExecutor::GetCommandName() const
{
    return "get";
}

////////////////////////////////////////////////////////////////////////////////

TSetExecutor::TSetExecutor()
    : PathArg("path", "path to an object in Cypress that must be set", true, "", "YPATH")
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
    : PathArg("path", "path to an object in Cypress that must be removed", true, "", "YPATH")
{
    CmdLine.add(PathArg);
}

void TRemoveExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path);

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TRemoveExecutor::GetCommandName() const
{
    return "remove";
}

////////////////////////////////////////////////////////////////////////////////

TListExecutor::TListExecutor()
    : PathArg("path", "path to a object in Cypress whose children must be listed", true, "", "YPATH")
{
    CmdLine.add(PathArg);
}

void TListExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path);

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TListExecutor::GetCommandName() const
{
    return "list";
}

////////////////////////////////////////////////////////////////////////////////

TCreateExecutor::TCreateExecutor()
    : TypeArg("type", "type of node", true, NObjectClient::EObjectType::Null, "NODE_TYPE")
    , PathArg("path", "path for a new object in Cypress", true, "", "YPATH")
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
}

Stroka TCreateExecutor::GetCommandName() const
{
    return "create";
}

////////////////////////////////////////////////////////////////////////////////

TLockExecutor::TLockExecutor()
    : TTransactedExecutor(true)
    , PathArg("path", "path to an object in Cypress that must be locked", true, "", "YPATH")
    , ModeArg("", "mode", "lock mode", false, NCypressClient::ELockMode::Exclusive, "snapshot, shared, exclusive")
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

TCopyExecutor::TCopyExecutor()
    : TTransactedExecutor(false)
    , SourcePathArg("src_path", "path to a source object in Cypress", true, "", "YPATH")
    , DestinationPathArg("dst_path", "destination path in Cypress", true, "", "YPATH")
{
    CmdLine.add(SourcePathArg);
    CmdLine.add(DestinationPathArg);
}

void TCopyExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto sourcePath = PreprocessYPath(SourcePathArg.getValue());
    auto destinationPath = PreprocessYPath(DestinationPathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("source_path").Scalar(sourcePath)
        .Item("destination_path").Scalar(destinationPath);

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TCopyExecutor::GetCommandName() const
{
    return "copy";
}

////////////////////////////////////////////////////////////////////////////////

TMoveExecutor::TMoveExecutor()
    : TTransactedExecutor(false)
    , SourcePathArg("src_path", "path to a source object in Cypress", true, "", "YPATH")
    , DestinationPathArg("dst_path", "destination path in Cypress", true, "", "YPATH")
{
    CmdLine.add(SourcePathArg);
    CmdLine.add(DestinationPathArg);
}

void TMoveExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto sourcePath = PreprocessYPath(SourcePathArg.getValue());
    auto destinationPath = PreprocessYPath(DestinationPathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("source_path").Scalar(sourcePath)
        .Item("destination_path").Scalar(destinationPath);

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TMoveExecutor::GetCommandName() const
{
    return "move";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
