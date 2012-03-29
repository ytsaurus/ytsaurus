#include "arguments.h"

#include <ytlib/ytree/lexer.h>

#include <build.h>

namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TArgsBase::TArgsBase()
    : Cmd("Command line", ' ', YT_VERSION)
    , ConfigArg("", "config", "configuration file", false, "", "file_name")
    , OutputFormatArg("", "format", "output format", false, TFormat(), "text, pretty, binary")
    , ConfigUpdatesArg("", "set", "set custom updates in config", false, "ypath=value")
    , OptsArg("", "opts", "other options", false, "options")
{
    Cmd.add(ConfigArg);
    Cmd.add(OptsArg);
    Cmd.add(OutputFormatArg);
    Cmd.add(ConfigUpdatesArg);
}

void TArgsBase::Parse(std::vector<std::string>& args)
{
    Cmd.parse(args);
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

void TArgsBase::ApplyConfigUpdates(NYTree::IYPathService* service)
{
    FOREACH (auto updateString, ConfigUpdatesArg.getValue()) {
        TYPath ypath;

        TToken token;
        while ((token = ChopToken(updateString, &updateString)).GetType() != ETokenType::Equals) {
            if (token.GetType() == ETokenType::None) {
                ythrow yexception() << "Incorrect option";
            }
            ypath += token.ToString();
        }

        SyncYPathSet(service, ypath, updateString);
    }

}

void TArgsBase::BuildOpts(IYsonConsumer* consumer)
{
    FOREACH (auto opts, OptsArg.getValue()) {
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
    : TxArg("", "tx", "transaction id", false, NObjectServer::NullTransactionId, "guid")
{
    Cmd.add(TxArg);
}

void TTransactedArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("transaction_id")
        .Scalar(TxArg.getValue().ToString());
    TArgsBase::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TGetArgs::TGetArgs()
    : PathArg("path", "path in Cypress", true, "", "path")
{
    Cmd.add(PathArg);
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
    : PathArg("path", "path in Cypress", true, "", "path")
    , ValueArg("value", "value to set", true, "", "yson")
{
    Cmd.add(PathArg);
    Cmd.add(ValueArg);
}

void TSetArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("set")
        .Item("path").Scalar(PathArg.getValue())
        .Item("value").OnNode(DeserializeFromYson(ValueArg.getValue()));

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TRemoveArgs::TRemoveArgs()
    : PathArg("path", "path in Cypress", true, "", "path")
{
    Cmd.add(PathArg);
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
    : PathArg("path", "path in Cypress", true, "", "path")
{
    Cmd.add(PathArg);
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
    : PathArg("path", "path in Cypress", true, "", "path")
    , TypeArg("type", "type of node", true, NObjectServer::EObjectType::Undefined, "object type")
    , ManifestArg("", "manifest", "manifest", false, "", "yson")
{
    Cmd.add(PathArg);
    Cmd.add(TypeArg);
    Cmd.add(ManifestArg);
}

void TCreateArgs::BuildCommand(IYsonConsumer* consumer)
{
    auto manifestYson = ManifestArg.getValue();

    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("create")
        .Item("path").Scalar(PathArg.getValue())
        .Item("type").Scalar(TypeArg.getValue().ToString())
        .DoIf(!manifestYson.empty(), [=] (TFluentMap fluent) {
            fluent.Item("manifest").OnNode(DeserializeFromYson(manifestYson));
         });

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TLockArgs::TLockArgs()
    : PathArg("path", "path in Cypress", true, "", "path")
    , ModeArg("", "mode", "lock mode", false, NCypress::ELockMode::Exclusive, "snapshot, shared, exclusive")
{
    Cmd.add(PathArg);
    Cmd.add(ModeArg);
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
    Cmd.add(ManifestArg);
}

void TStartTxArgs::BuildCommand(IYsonConsumer* consumer)
{
    auto manifestYson = ManifestArg.getValue();
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
    : PathArg("path", "path in Cypress", true, "", "path")
{
    Cmd.add(PathArg);
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
    : PathArg("path", "path in Cypress", true, "", "path")
    , ValueArg("value", "value to set", true, "", "yson")
{
    Cmd.add(PathArg);
    Cmd.add(ValueArg);
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
        .Item("path").Scalar(PathArg.getValue())
        .Item("value").OnNode(DeserializeFromYson(ValueArg.getValue()));

    TTransactedArgs::BuildCommand(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TUploadArgs::TUploadArgs()
    : PathArg("path", "path in Cypress", true, "", "path")
{
    Cmd.add(PathArg);
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
    : PathArg("path", "path in Cypress", true, "", "path")
{
    Cmd.add(PathArg);
}

void TDownloadArgs::BuildCommand(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("do").Scalar("download")
        .Item("path").Scalar(PathArg.getValue());

    TTransactedArgs::BuildCommand(consumer);
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
