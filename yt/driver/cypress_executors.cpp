#include "cypress_executors.h"
#include "preprocess.h"

#include <ytlib/driver/driver.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TGetExecutor::TGetExecutor()
    : PathArg("path", "object path to get", true, TRichYPath(""), "YPATH")
    , AttributeArg("", "attr", "attribute key to fetch", false, "ATTRIBUTE")
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
    : PathArg("path", "object path to set", true, TRichYPath(""), "YPATH")
    , ValueArg("value", "value to set", false, "", "YSON")
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
    : PathArg("path", "object path to remove", true, TRichYPath(""), "YPATH")
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
    : PathArg("path", "collection path to list", true, TRichYPath(""), "YPATH")
    , AttributeArg("", "attr", "attribute key to fetch", false, "ATTRIBUTE")
{
    CmdLine.add(PathArg);
    CmdLine.add(AttributeArg);
}

void TListExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Scalar(path)
        .Item("attributes").List(AttributeArg);

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TListExecutor::GetCommandName() const
{
    return "list";
}

////////////////////////////////////////////////////////////////////////////////

TCreateExecutor::TCreateExecutor()
    : TypeArg("type", "type of node", true, NObjectClient::EObjectType::Null, "NODE_TYPE")
    , PathArg("path", "object path to create", true, TRichYPath(""), "YPATH")
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
    , PathArg("path", "object path to lock", true, TRichYPath(""), "YPATH")
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
    , SourcePathArg("src_path", "source object path", true, TRichYPath(""), "YPATH")
    , DestinationPathArg("dst_path", "destination object path", true, TRichYPath(""), "YPATH")
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
    , SourcePathArg("src_path", "source object path", true, TRichYPath(""), "YPATH")
    , DestinationPathArg("dst_path", "destination object path", true, TRichYPath(""), "YPATH")
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

TExistsExecutor::TExistsExecutor()
    : TTransactedExecutor(false)
    , PathArg("path", "path to check", true, TRichYPath(""), "YPATH")
{
    CmdLine.add(PathArg);
}

void TExistsExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer).Item("path").Scalar(path);

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TExistsExecutor::GetCommandName() const
{
    return "exists";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
