#include "arguments.h"

namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TArgsBase::TArgsBase()
{
    CmdLine.Reset(new TCLAP::CmdLine("Command line"));

    ConfigArg.Reset(new TCLAP::ValueArg<Stroka>(
        "", "config", "configuration file", false, "", "file_name"));
    OutputFormatArg.Reset(new TCLAP::ValueArg<EYsonFormat>(
        "", "format", "output format", false, EYsonFormat::Text, "text, pretty, binary"));
    ConfigUpdatesArg.Reset(new TCLAP::MultiArg<Stroka>(
        "", "config_set", "set configuration value", false, "ypath=yson"));
    OptsArg.Reset(new TCLAP::MultiArg<Stroka>(
        "", "opts", "other options", false, "key=yson"));

    CmdLine->add(~ConfigArg);
    CmdLine->add(~OptsArg);
    CmdLine->add(~OutputFormatArg);
    CmdLine->add(~ConfigUpdatesArg);
}

void TArgsBase::Parse(std::vector<std::string>& args)
{
    CmdLine->parse(args);
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
    return Stroka(ConfigArg->getValue());
}

EYsonFormat TArgsBase::GetOutputFormat()
{
    return OutputFormatArg->getValue();
}

void TArgsBase::ApplyConfigUpdates(NYTree::IYPathServicePtr service)
{
    FOREACH (auto updateString, ConfigUpdatesArg->getValue()) {
        int index = updateString.find_first_of('=');
        auto ypath = updateString.substr(0, index);
        auto yson = updateString.substr(index + 1);
        SyncYPathSet(service, ypath, yson);
    }

}

void TArgsBase::BuildOptions(IYsonConsumer* consumer, TCLAP::MultiArg<Stroka>* arg)
{
    // TODO(babenko): think about a better way of doing this
    FOREACH (auto opts, arg->getValue()) {
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
    BuildOptions(consumer, OptsArg.Get());
}

////////////////////////////////////////////////////////////////////////////////

TTransactedArgs::TTransactedArgs()
{
    TxArg.Reset(new TTxArg("", "tx", "set transaction id", false, NObjectServer::NullTransactionId, "transaction_id"));
    CmdLine->add(~TxArg);
}

void TTransactedArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("transaction_id").Scalar(TxArg->getValue().ToString());
    TArgsBase::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TGetArgs::TGetArgs()
{
    PathArg.Reset(new TUnlabeledStringArg("path", "path to an object in Cypress that must be retrieved", true, "", "path"));
    CmdLine->add(~PathArg);
}

void TGetArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("get")
        .Item("path").Scalar(PathArg->getValue());

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TSetArgs::TSetArgs()
{
    PathArg.Reset(new TUnlabeledStringArg("path", "path to an object in Cypress that must be set", true, "", "path"));
    ValueArg.Reset(new TUnlabeledStringArg("value", "value to set", true, "", "yson"));

    CmdLine->add(~PathArg);
    CmdLine->add(~ValueArg);
}

void TSetArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("set")
        .Item("path").Scalar(PathArg->getValue())
        .Item("value").Node(ValueArg->getValue());

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TRemoveArgs::TRemoveArgs()
{
    PathArg.Reset(new TUnlabeledStringArg("path", "path to an object in Cypress that must be removed", true, "", "path"));
    CmdLine->add(~PathArg);
}

void TRemoveArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("remove")
        .Item("path").Scalar(PathArg->getValue());

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TListArgs::TListArgs()
{
    PathArg.Reset(new TUnlabeledStringArg("path", "path to a object in Cypress whose children must be listed", true, "", "path"));
    CmdLine->add(~PathArg);
}

void TListArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("list")
        .Item("path").Scalar(PathArg->getValue());

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TCreateArgs::TCreateArgs()
{
    PathArg.Reset(new TUnlabeledStringArg("path", "path for a new object in Cypress", true, "", "ypath"));
    TypeArg.Reset(new TTypeArg(
        "type", "type of node", true, NObjectServer::EObjectType::Undefined, "object type"));

    CmdLine->add(~TypeArg);
    CmdLine->add(~PathArg);

    ManifestArg.Reset(new TManifestArg("", "manifest", "manifest", false, "", "yson"));
    CmdLine->add(~ManifestArg);
}

void TCreateArgs::BuildCommand(IYsonConsumer* consumer)
{
    auto manifestYson = ManifestArg->getValue();

    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("create")
        .Item("path").Scalar(PathArg->getValue())
        .Item("type").Scalar(TypeArg->getValue().ToString())
        .DoIf(!manifestYson.empty(), [=] (TFluentMap fluent) {
            fluent.Item("manifest").Node(manifestYson);
         });

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TLockArgs::TLockArgs()
{
    PathArg.Reset(new TUnlabeledStringArg("path", "path to an object in Cypress that must be locked", true, "", "path"));
    ModeArg.Reset(new TModeArg("", "mode", "lock mode", false, NCypress::ELockMode::Exclusive, "snapshot, shared, exclusive"));

    CmdLine->add(~PathArg);
    CmdLine->add(~ModeArg);
}

void TLockArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("lock")
        .Item("path").Scalar(PathArg->getValue())
        .Item("mode").Scalar(ModeArg->getValue().ToString());

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TStartTxArgs::TStartTxArgs()
{
    ManifestArg.Reset(new TManifestArg("", "manifest", "manifest", false, "", "yson"));
    CmdLine->add(~ManifestArg);
}

void TStartTxArgs::BuildCommand(IYsonConsumer* consumer)
{
    auto manifestYson = ManifestArg->getValue();
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
{
    PathArg.Reset(new TUnlabeledStringArg("path", "path to a table in Cypress that must be read", true, "", "ypath"));
    CmdLine->add(~PathArg);
}

void TReadArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("read")
        .Item("path").Scalar(PathArg->getValue());

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TWriteArgs::TWriteArgs()
{
    PathArg.Reset(new TUnlabeledStringArg("path", "path to a table in Cypress that must be written", true, "", "ypath"));
    CmdLine->add(~PathArg);

    ValueArg.Reset(new TUnlabeledStringArg("value", "row(s) to write", true, "", "yson"));
    CmdLine->add(~ValueArg);
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
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("write")
        .Item("path").Scalar(PathArg->getValue())
        .Item("value").Node(ValueArg->getValue());

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TUploadArgs::TUploadArgs()
{
    PathArg.Reset(new TUnlabeledStringArg("path", "to a new file in Cypress that must be uploaded", true, "", "ypath"));
    CmdLine->add(~PathArg);
}

void TUploadArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("upload")
        .Item("path").Scalar(PathArg->getValue());

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TDownloadArgs::TDownloadArgs()
{
    PathArg.Reset(new TUnlabeledStringArg("path", "path to a file in Cypress that must be downloaded", true, "", "ypath"));
    CmdLine->add(~PathArg);
}

void TDownloadArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("download")
        .Item("path").Scalar(PathArg->getValue());

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TMapArgs::TMapArgs()
{
    InArg.Reset(new TCLAP::MultiArg<Stroka>("", "in", "input tables", false, "ypath"));
    CmdLine->add(~InArg);

    OutArg.Reset(new TCLAP::MultiArg<Stroka>("", "out", "output tables", false, "ypath"));
    CmdLine->add(~OutArg);

    FilesArg.Reset(new TCLAP::MultiArg<Stroka>("", "file", "additional files", false, "ypath"));
    CmdLine->add(~FilesArg);

    ShellCommandArg.Reset(new TCLAP::ValueArg<Stroka>("", "command", "shell command", true, "", "path"));
    CmdLine->add(~ShellCommandArg);
}

void TMapArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("map")
        .Item("spec").BeginMap()
            .Item("shell_command").Scalar(ShellCommandArg->getValue())
            .Item("in").List(InArg->getValue())
            .Item("out").List(OutArg->getValue())
            .Item("files").List(FilesArg->getValue())
        .EndMap();

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
