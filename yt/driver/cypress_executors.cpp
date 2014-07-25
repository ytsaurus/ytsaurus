#include "cypress_executors.h"
#include "preprocess.h"

#include <ytlib/driver/driver.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NYson;
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
        .Item("path").Value(path)
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
        .Item("path").Value(path);

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
    , NonRecursiveArg("", "non_recursive", "check that removed node is empty", false)
    , ForceArg("", "force", "do not throw if path does not exist", false)
{
    CmdLine.add(PathArg);
    CmdLine.add(NonRecursiveArg);
    CmdLine.add(ForceArg);
}

void TRemoveExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Value(path)
        .Item("recursive").Value(!NonRecursiveArg.getValue())
        .Item("force").Value(ForceArg.getValue());

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
        .Item("path").Value(path)
        .Item("attributes").List(AttributeArg);

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TListExecutor::GetCommandName() const
{
    return "list";
}

////////////////////////////////////////////////////////////////////////////////

TCreateExecutor::TCreateExecutor()
    : TypeArg("type", "object type to create", true, NObjectClient::EObjectType::Null, "OBJECT_TYPE")
    , PathArg("path", "object path to create", false, TRichYPath(""), "YPATH")
    , RecursiveArg("", "recursive", "create nodes of path recursively", false)
    , IgnoreExistingArg("", "ignore_existing", "do not fail if node already exists and has the same type", false)
{
    CmdLine.add(TypeArg);
    CmdLine.add(PathArg);
    CmdLine.add(RecursiveArg);
    CmdLine.add(IgnoreExistingArg);
}

void TCreateExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path =
        PathArg.isSet()
        ? TNullable<TRichYPath>(PreprocessYPath(PathArg.getValue()))
        : Null;

    BuildYsonMapFluently(consumer)
        .DoIf(path.HasValue(), [&] (TFluentMap fluent) {
            fluent.Item("path").Value(path.Get());
        })
        .Item("type").Value(FormatEnum(TypeArg.getValue()))
        .Item("recursive").Value(RecursiveArg.getValue())
        .Item("ignore_existing").Value(IgnoreExistingArg.getValue());

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
    , WaitableArg("", "waitable", "wait until the lock could be acquired", false)
{
    CmdLine.add(PathArg);
    CmdLine.add(ModeArg);
    CmdLine.add(WaitableArg);
}

void TLockExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto path = PreprocessYPath(PathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("path").Value(path)
        .Item("mode").Value(FormatEnum(ModeArg.getValue()))
        .Item("waitable").Value(WaitableArg.getValue());

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
    , PreserveAccountArg("", "preserve_account", "keep the original account of copied nodes", false)
{
    CmdLine.add(SourcePathArg);
    CmdLine.add(DestinationPathArg);
    CmdLine.add(PreserveAccountArg);
}

void TCopyExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto sourcePath = PreprocessYPath(SourcePathArg.getValue());
    auto destinationPath = PreprocessYPath(DestinationPathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("source_path").Value(sourcePath)
        .Item("destination_path").Value(destinationPath)
        .Item("preserve_account").Value(PreserveAccountArg.getValue());

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
        .Item("source_path").Value(sourcePath)
        .Item("destination_path").Value(destinationPath);

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

    BuildYsonMapFluently(consumer)
        .Item("path").Value(path);

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TExistsExecutor::GetCommandName() const
{
    return "exists";
}

////////////////////////////////////////////////////////////////////////////////

TLinkExecutor::TLinkExecutor()
    : TargetPathArg("target_path", "path to target object", true, TRichYPath(""), "YPATH")
    , LinkPathArg("link_path", "path to link node", true, TRichYPath(""), "YPATH")
    , RecursiveArg("", "recursive", "create nodes of path recursively", false)
    , IgnoreExistingArg("", "ignore_existing", "do not fail if node already exists and has the same type", false)
{
    CmdLine.add(TargetPathArg);
    CmdLine.add(LinkPathArg);
    CmdLine.add(RecursiveArg);
    CmdLine.add(IgnoreExistingArg);
}

void TLinkExecutor::BuildArgs(IYsonConsumer* consumer)
{
    auto targetPath = PreprocessYPath(TargetPathArg.getValue());
    auto linkPath = PreprocessYPath(LinkPathArg.getValue());

    BuildYsonMapFluently(consumer)
        .Item("target_path").Value(targetPath)
        .Item("link_path").Value(linkPath)
        .Item("recursive").Value(RecursiveArg.getValue())
        .Item("ignore_existing").Value(IgnoreExistingArg.getValue());

    TTransactedExecutor::BuildArgs(consumer);
}

Stroka TLinkExecutor::GetCommandName() const
{
    return "link";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
