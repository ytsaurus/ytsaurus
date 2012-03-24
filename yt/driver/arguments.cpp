#include "arguments.h"

namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TArgsBase::TArgsBase()
{
    Cmd.Reset(new TCLAP::CmdLine("Command line"));

    ConfigArg.Reset(new TCLAP::ValueArg<std::string>(
        "", "config", "configuration file", false, "", "file_name"));
    OutputFormatArg.Reset(new TCLAP::ValueArg<EYsonFormat>(
        "", "format", "output format", false, EYsonFormat::Text, "text, pretty, binary"));

    ConfigUpdatesArg.Reset(new TCLAP::MultiArg<Stroka>(
        "", "set", "set custom updates in config", false, "ypath=value"));

    OptsArg.Reset(new TCLAP::MultiArg<std::string>(
        "", "opts", "other options", false, "options"));

    Cmd->add(~ConfigArg);
    Cmd->add(~OptsArg);
    Cmd->add(~OutputFormatArg);
    Cmd->add(~ConfigUpdatesArg);
}

void TArgsBase::Parse(std::vector<std::string>& args)
{
    Cmd->parse(args);
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

void TArgsBase::ApplyConfigUpdates(NYTree::IYPathService* service)
{
    FOREACH (auto updateString, ConfigUpdatesArg->getValue()) {
        int index = updateString.find_first_of('=');
        auto ypath = updateString.substr(0, index);
        auto yson = updateString.substr(index + 1);
        SyncYPathSet(service, ypath, yson);
    }

}

void TArgsBase::BuildOpts(IYsonConsumer* consumer)
{
    FOREACH (auto opts, OptsArg->getValue()) {
        NYTree::TYson yson = Stroka("{") + Stroka(opts) + "}";
        auto items = NYTree::DeserializeFromYson(yson)->AsMap();
        FOREACH (auto child, items->GetChildren()) {
            consumer->OnMapItem(child.first);
            consumer->OnStringScalar(child.second->AsString()->GetValue());
        }
    }
}

void TArgsBase::BuildCommand(IYsonConsumer* consumer)
{
    BuildOpts(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TTransactedArgs::TTransactedArgs()
{
    TxArg.Reset(new TTxArg(
        "", "tx", "transaction id", false, NObjectServer::NullTransactionId, "guid"));
    Cmd->add(~TxArg);
}

void TTransactedArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("transaction_id")
        .Scalar(TxArg->getValue().ToString());
    TArgsBase::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TGetArgs::TGetArgs()
{
    PathArg.Reset(new TFreeStringArg("path", "path in Cypress", true, "", "path"));
    Cmd->add(~PathArg);
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
    PathArg.Reset(new TFreeStringArg("path", "path in Cypress", true, "", "path"));
    ValueArg.Reset(new TFreeStringArg("value", "value to set", true, "", "yson"));

    Cmd->add(~PathArg);
    Cmd->add(~ValueArg);
}

void TSetArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("set")
        .Item("path").Scalar(PathArg->getValue())
        .Item("value").OnNode(DeserializeFromYson(ValueArg->getValue()));

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TRemoveArgs::TRemoveArgs()
{
    PathArg.Reset(new TFreeStringArg("path", "path in Cypress", true, "", "path"));
    Cmd->add(~PathArg);
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
    PathArg.Reset(new TFreeStringArg("path", "path in Cypress", true, "", "path"));
    Cmd->add(~PathArg);
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
    PathArg.Reset(new TFreeStringArg("path", "path in Cypress", true, "", "path"));
    TypeArg.Reset(new TTypeArg(
        "type", "type of node", true, NObjectServer::EObjectType::Undefined, "object type"));

    Cmd->add(~PathArg);
    Cmd->add(~TypeArg);

    ManifestArg.Reset(new TManifestArg("", "manifest", "manifest", false, "", "yson"));
    Cmd->add(~ManifestArg);
}

void TCreateArgs::BuildCommand(IYsonConsumer* consumer)
{
    auto manifestYson = ManifestArg->getValue();

    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("create")
        .Item("path").Scalar(PathArg->getValue())
        .Item("type").Scalar(TypeArg->getValue().ToString())
        .DoIf(!manifestYson.empty(), [=] (TFluentMap fluent) {
            fluent.Item("manifest").OnNode(DeserializeFromYson(manifestYson));
         });

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TLockArgs::TLockArgs()
{
    PathArg.Reset(new TFreeStringArg("path", "path in Cypress", true, "", "path"));
    ModeArg.Reset(new TModeArg(
        "", "mode", "lock mode", false, NCypress::ELockMode::Exclusive, "snapshot, shared, exclusive"));
    Cmd->add(~PathArg);
    Cmd->add(~ModeArg);
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
    Cmd->add(~ManifestArg);
}

void TStartTxArgs::BuildCommand(IYsonConsumer* consumer)
{
    auto manifestYson = ManifestArg->getValue();
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("start")
        .DoIf(!manifestYson.empty(), [=] (TFluentMap fluent) {
            fluent.Item("manifest").OnNode(DeserializeFromYson(manifestYson));
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
    PathArg.Reset(new TFreeStringArg("path", "path in Cypress", true, "", "path"));
    Cmd->add(~PathArg);
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
    PathArg.Reset(new TFreeStringArg("path", "path in Cypress", true, "", "path"));
    Cmd->add(~PathArg);

    ValueArg.Reset(new TFreeStringArg("value", "value to set", true, "", "yson"));
    Cmd->add(~ValueArg);
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
        .Item("value").OnNode(DeserializeFromYson(ValueArg->getValue()));

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TUploadArgs::TUploadArgs()
{
    PathArg.Reset(new TFreeStringArg("path", "path in Cypress", true, "", "path"));
    Cmd->add(~PathArg);
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
    PathArg.Reset(new TFreeStringArg("path", "path in Cypress", true, "", "path"));
    Cmd->add(~PathArg);
}

void TDownloadArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("download")
        .Item("path").Scalar(PathArg->getValue());

    TTransactedArgs::BuildCommand(consumer);
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
