#include "arguments.h"

#include <build.h>

#include <ytlib/job_proxy/config.h>

namespace NYT {

using namespace NYTree;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

TArgsBase::TArgsBase()
    : CmdLine("Command line", ' ', YT_VERSION)
    , ConfigArg("", "config", "configuration file", false, "", "file_name")
    , OutputFormatArg("", "format", "output format", false, TFormat(), "text, pretty, binary")
    , ConfigUpdatesArg("", "config_set", "set configuration value", false, "ypath=yson")
    , OptsArg("", "opts", "other options", false, "key=yson")
{
    CmdLine.add(ConfigArg);
    CmdLine.add(OptsArg);
    CmdLine.add(OutputFormatArg);
    CmdLine.add(ConfigUpdatesArg);
}

void TArgsBase::Parse(std::vector<std::string>& args)
{
    CmdLine.parse(args);
}

INodePtr TArgsBase::GetCommand()
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    builder->OnBeginMap();
    BuildCommand(~builder);
    builder->OnEndMap();
    return builder->EndTree();
}

Stroka TArgsBase::GetConfigName()
{
    return Stroka(ConfigArg.getValue());
}

TArgsBase::TFormat TArgsBase::GetOutputFormat()
{
    return OutputFormatArg.getValue();
}

void TArgsBase::ApplyConfigUpdates(NYTree::IYPathServicePtr service)
{
    FOREACH (auto updateString, ConfigUpdatesArg.getValue()) {
        int index = updateString.find_first_of('=');
        auto ypath = updateString.substr(0, index);
        auto yson = updateString.substr(index + 1);
        SyncYPathSet(service, ypath, yson);
    }
}

void TArgsBase::BuildOptions(IYsonConsumer* consumer, TCLAP::MultiArg<Stroka>& arg)
{
    // TODO(babenko): think about a better way of doing this
    FOREACH (const auto& opts, arg.getValue()) {
        NYTree::TYson yson = Stroka("{") + Stroka(opts) + "}";
        auto items = NYTree::DeserializeFromYson(yson)->AsMap();
        FOREACH (const auto& pair, items->GetChildren()) {
            consumer->OnMapItem(pair.first);
            VisitTree(pair.second, consumer, true);
        }
    }
}

void TArgsBase::BuildCommand(IYsonConsumer* consumer)
{
    BuildOptions(consumer, OptsArg);
}

////////////////////////////////////////////////////////////////////////////////

TTransactedArgs::TTransactedArgs()
    : TxArg("", "tx", "set transaction id", false, NObjectServer::NullTransactionId, "transaction_id")
{
    CmdLine.add(TxArg);
}

void TTransactedArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("transaction_id").Scalar(TxArg.getValue());
    TArgsBase::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TGetArgs::TGetArgs()
    : PathArg("path", "path to an object in Cypress that must be retrieved", true, "", "path")
{
    CmdLine.add(PathArg);
}

void TGetArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("get")
        .Item("path").Scalar(PathArg.getValue());

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TSetArgs::TSetArgs()
    : PathArg("path", "path to an object in Cypress that must be set", true, "", "path")
    , ValueArg("value", "value to set", true, "", "yson")
{
    CmdLine.add(PathArg);
    CmdLine.add(ValueArg);
}

void TSetArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("set")
        .Item("path").Scalar(PathArg.getValue())
        .Item("value").Node(ValueArg.getValue());

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TRemoveArgs::TRemoveArgs()
    : PathArg("path", "path to an object in Cypress that must be removed", true, "", "path")
{
    CmdLine.add(PathArg);
}

void TRemoveArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("remove")
        .Item("path").Scalar(PathArg.getValue());

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TListArgs::TListArgs()
    : PathArg("path", "path to a object in Cypress whose children must be listed", true, "", "path")
{
    CmdLine.add(PathArg);
}

void TListArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("list")
        .Item("path").Scalar(PathArg.getValue());

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TCreateArgs::TCreateArgs()
    : TypeArg("type", "type of node", true, NObjectServer::EObjectType::Undefined, "object type")
    , PathArg("path", "path for a new object in Cypress", true, "", "ypath")
    , ManifestArg("", "manifest", "manifest", false, "", "yson")
{
    CmdLine.add(TypeArg);
    CmdLine.add(PathArg);
    CmdLine.add(ManifestArg);
}

void TCreateArgs::BuildCommand(IYsonConsumer* consumer)
{
    auto manifestYson = ManifestArg.getValue();

    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("create")
        .Item("path").Scalar(PathArg.getValue())
        .Item("type").Scalar(TypeArg.getValue().ToString())
        .DoIf(!manifestYson.empty(), [=] (TFluentMap fluent) {
            fluent.Item("manifest").Node(manifestYson);
         });

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TLockArgs::TLockArgs()
    : PathArg("path", "path to an object in Cypress that must be locked", true, "", "path")
    , ModeArg("", "mode", "lock mode", false, NCypress::ELockMode::Exclusive, "snapshot, shared, exclusive")
{
    CmdLine.add(PathArg);
    CmdLine.add(ModeArg);
}

void TLockArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("lock")
        .Item("path").Scalar(PathArg.getValue())
        .Item("mode").Scalar(ModeArg.getValue().ToString());

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TStartTxArgs::TStartTxArgs()
    : ManifestArg("", "manifest", "manifest", false, "", "yson")
{
    CmdLine.add(ManifestArg);
}

void TStartTxArgs::BuildCommand(IYsonConsumer* consumer)
{
    auto manifestYson = ManifestArg.getValue();
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("start")
        .DoIf(!manifestYson.empty(), [=] (TFluentMap fluent) {
            fluent.Item("manifest").Node(manifestYson);
         });
    TArgsBase::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

void TCommitTxArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("commit");

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

void TAbortTxArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("abort");

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TReadArgs::TReadArgs()
    : PathArg("path", "path to a table in Cypress that must be read", true, "", "ypath")
{
    CmdLine.add(PathArg);
}

void TReadArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("read")
        .Item("path").Scalar(PathArg.getValue());

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TWriteArgs::TWriteArgs()
    : PathArg("path", "path to a table in Cypress that must be written", true, "", "ypath")
    , ValueArg("value", "row(s) to write", false, "", "yson")
    , SortedArg("s", "sorted", "create sorted table (table must initially be empty, input data must be sorted)")
{
    CmdLine.add(PathArg);
    CmdLine.add(ValueArg);
}

    // TODO(panin): validation?
//    virtual void DoValidate() const
//    {
//        if (Value) {
//            auto type = Value->GetType();
//            if (type != NYTree::ENodeType::List && type != NYTree::ENodeType::Map) {
//                ythrow yexception() << "\"value\" must be a list or a map";
//            }
//        }
//    }

void TWriteArgs::BuildCommand(IYsonConsumer* consumer)
{
    auto value = ValueArg.getValue();

    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("write")
        .Item("path").Scalar(PathArg.getValue())
        .DoIf(!value.empty(), [=] (TFluentMap fluent) {
                fluent.Item("value").Node(value);
        });

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TUploadArgs::TUploadArgs()
    : PathArg("path", "to a new file in Cypress that must be uploaded", true, "", "ypath")
{
    CmdLine.add(PathArg);
}

void TUploadArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("upload")
        .Item("path").Scalar(PathArg.getValue());

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TDownloadArgs::TDownloadArgs()
    : PathArg("path", "path to a file in Cypress that must be downloaded", true, "", "ypath")
{
    CmdLine.add(PathArg);
}

void TDownloadArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("download")
        .Item("path").Scalar(PathArg.getValue());

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TMapArgs::TMapArgs()
    : InArg("", "in", "input tables", false, "ypath")
    , OutArg("", "out", "output tables", false, "ypath")
    , FilesArg("", "file", "additional files", false, "ypath")
    , MapperArg("", "mapper", "mapper shell command", true, "", "command")
{
    CmdLine.add(InArg);
    CmdLine.add(OutArg);
    CmdLine.add(FilesArg);
    CmdLine.add(MapperArg);
}

void TMapArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("map")
        .Item("spec").BeginMap()
            .Item("mapper").Scalar(MapperArg.getValue())
            .Item("input_table_paths").List(InArg.getValue())
            .Item("output_table_paths").List(OutArg.getValue())
            .Item("files").List(FilesArg.getValue())
        .EndMap();

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TMergeArgs::TMergeArgs()
    : InArg("", "in", "input tables", false, "ypath")
    , OutArg("", "out", "output table", false, "", "ypath")
    , ModeArg("", "mode", "merge mode", false, TMode(EMergeMode::Unordered), "unordered, ordered, sorted")
    , CombineArg("", "combine", "combine small output chunks into larger ones")
{
    CmdLine.add(InArg);
    CmdLine.add(OutArg);
    CmdLine.add(ModeArg);
    CmdLine.add(CombineArg);
}

void TMergeArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("merge")
        .Item("spec").BeginMap()
            .Item("input_table_paths").List(InArg.getValue())
            .Item("output_table_path").Scalar(OutArg.getValue())
            .Item("mode").Scalar(FormatEnum(ModeArg.getValue().Get()))
            .Item("combine_chunks").Scalar(CombineArg.getValue())
        .EndMap();

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
