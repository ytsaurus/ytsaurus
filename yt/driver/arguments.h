#pragma once

#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/serialize.h>

#include <tclap/CmdLine.h>
#include <ytlib/misc/tclap_helpers.h>

namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

//TODO(panin): move to proper place
class TArgsBase
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TArgsBase> TPtr;

    TArgsBase()
    {
        Cmd.Reset(new TCLAP::CmdLine("Command line"));
        ConfigArg.Reset(new TCLAP::ValueArg<std::string>(
            "", "config", "configuration file", false, "", "file_name"));
        OptsArg.Reset(new TCLAP::MultiArg<std::string>(
            "", "opts", "other options", false, "options"));

        Cmd->add(~ConfigArg);
        Cmd->add(~OptsArg);
    }

    void Parse(std::vector<std::string>& args)
    {
        Cmd->parse(args);
    }

    INodePtr GetCommand()
    {
        auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
        builder->BeginTree();
        builder->OnBeginMap();
        BuildCommand(~builder);
        builder->OnEndMap();
        return builder->EndTree();
    }

    Stroka GetConfigName()
    {
        return Stroka(ConfigArg->getValue());
    }

protected:
    //useful typedefs
    typedef TCLAP::UnlabeledValueArg<Stroka> TFreeStringArg;

    THolder<TCLAP::CmdLine> Cmd;

    THolder<TCLAP::ValueArg<std::string> > ConfigArg;
    THolder<TCLAP::MultiArg<std::string> > OptsArg;

    void BuildOpts(IYsonConsumer* consumer)
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

    virtual void BuildCommand(IYsonConsumer* consumer) {
        BuildOpts(consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////


class TTransactedArgs
    : public TArgsBase
{
public:
    TTransactedArgs()
    {
        TxArg.Reset(new TTxArg(
            "", "tx", "transaction id", false, NObjectServer::NullTransactionId, "guid"));
        Cmd->add(~TxArg);
    }
protected:
    typedef TCLAP::ValueArg<NObjectServer::TTransactionId> TTxArg;
    THolder<TTxArg> TxArg;

    virtual void BuildCommand(IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item("transaction_id")
            .Scalar(TxArg->getValue().ToString());
        TArgsBase::BuildCommand(consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TGetArgs
    : public TTransactedArgs
{
public:
    TGetArgs()
    {
        PathArg.Reset(new TFreeStringArg("path", "path in Cypress", true, "", "path"));
        Cmd->add(~PathArg);
    }

private:
    THolder<TFreeStringArg> PathArg;

    virtual void BuildCommand(IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item("do").Scalar("get")
            .Item("path").Scalar(PathArg->getValue());

        TTransactedArgs::BuildCommand(consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSetArgs
    : public TTransactedArgs
{
public:
    TSetArgs()
    {
        PathArg.Reset(new TFreeStringArg("path", "path in Cypress", true, "", "path"));
        ValueArg.Reset(new TFreeStringArg("value", "value to set", true, "", "yson"));

        Cmd->add(~PathArg);
        Cmd->add(~ValueArg);
    }

private:
    THolder<TFreeStringArg> PathArg;
    THolder<TFreeStringArg> ValueArg;

    virtual void BuildCommand(IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item("do").Scalar("set")
            .Item("path").Scalar(PathArg->getValue())
            .Item("value").OnNode(DeserializeFromYson(ValueArg->getValue()));

        TTransactedArgs::BuildCommand(consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRemoveArgs
    : public TTransactedArgs
{
public:
    TRemoveArgs()
    {
        PathArg.Reset(new TFreeStringArg("path", "path in Cypress", true, "", "path"));
        Cmd->add(~PathArg);
    }

private:
    THolder<TFreeStringArg> PathArg;

    virtual void BuildCommand(IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item("do").Scalar("remove")
            .Item("path").Scalar(PathArg->getValue());

        TTransactedArgs::BuildCommand(consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TListArgs
    : public TTransactedArgs
{
public:
    TListArgs()
    {
        PathArg.Reset(new TFreeStringArg("path", "path in Cypress", true, "", "path"));
        Cmd->add(~PathArg);
    }

private:
    THolder<TFreeStringArg> PathArg;

    virtual void BuildCommand(IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item("do").Scalar("list")
            .Item("path").Scalar(PathArg->getValue());

        TTransactedArgs::BuildCommand(consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCreateArgs
    : public TTransactedArgs
{
public:
    TCreateArgs()
    {
        PathArg.Reset(new TFreeStringArg("path", "path in Cypress", true, "", "path"));
        TypeArg.Reset(new TTypeArg(
            "type", "type of node", true, NObjectServer::EObjectType::Undefined, "object type"));

        Cmd->add(~PathArg);
        Cmd->add(~TypeArg);

        ManifestArg.Reset(new TManifestArg("", "manifest", "manifest", false, "", "yson"));
        Cmd->add(~ManifestArg);
    }

private:
    THolder<TFreeStringArg> PathArg;

    typedef TCLAP::UnlabeledValueArg<NObjectServer::EObjectType> TTypeArg;
    THolder<TTypeArg> TypeArg;

    typedef TCLAP::ValueArg<NYTree::TYson> TManifestArg;
    THolder<TManifestArg> ManifestArg;

    virtual void BuildCommand(IYsonConsumer* consumer)
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

};

////////////////////////////////////////////////////////////////////////////////

class TLockArgs
    : public TTransactedArgs
{
public:
    TLockArgs()
    {
        PathArg.Reset(new TFreeStringArg("path", "path in Cypress", true, "", "path"));
        ModeArg.Reset(new TModeArg(
            "", "mode", "lock mode", false, NCypress::ELockMode::Exclusive, "snapshot, shared, exclusive"));
        Cmd->add(~PathArg);
        Cmd->add(~ModeArg);
    }

private:
    THolder<TFreeStringArg> PathArg;

    typedef TCLAP::ValueArg<NCypress::ELockMode> TModeArg;
    THolder<TModeArg> ModeArg;

    virtual void BuildCommand(IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item("do").Scalar("lock")
            .Item("path").Scalar(PathArg->getValue())
            .Item("mode").Scalar(ModeArg->getValue().ToString());

        TTransactedArgs::BuildCommand(consumer);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TStartArgs
    : public TArgsBase
{
public:
    TStartArgs()
    {
        ManifestArg.Reset(new TManifestArg("", "manifest", "manifest", false, "", "yson"));
        Cmd->add(~ManifestArg);
    }

private:
    typedef TCLAP::ValueArg<NYTree::TYson> TManifestArg;
    THolder<TManifestArg> ManifestArg;

    virtual void BuildCommand(IYsonConsumer* consumer)
    {
        auto manifestYson = ManifestArg->getValue();
        BuildYsonMapFluently(consumer)
            .Item("do").Scalar("start")
            .DoIf(!manifestYson.empty(), [=] (TFluentMap fluent) {
                fluent.Item("manifest").OnNode(DeserializeFromYson(manifestYson));
             });
        TArgsBase::BuildCommand(consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCommitArgs
    : public TTransactedArgs
{
public:
    TCommitArgs()
    { }

private:
    virtual void BuildCommand(IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item("do").Scalar("commit");

        TTransactedArgs::BuildCommand(consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAbortArgs
    : public TTransactedArgs
{
public:
    TAbortArgs()
    { }

private:
    virtual void BuildCommand(IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item("do").Scalar("abort");

        TTransactedArgs::BuildCommand(consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TReadArgs
    : public TTransactedArgs
{
public:
    TReadArgs()
    {
        PathArg.Reset(new TFreeStringArg("path", "path in Cypress", true, "", "path"));
        Cmd->add(~PathArg);
    }

private:
    THolder<TFreeStringArg> PathArg;

    virtual void BuildCommand(IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item("do").Scalar("read")
            .Item("path").Scalar(PathArg->getValue());

        TTransactedArgs::BuildCommand(consumer);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TWriteArgs
    : public TTransactedArgs
{
public:
    TWriteArgs()
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

private:
    THolder<TFreeStringArg> PathArg;

    //TODO(panin):support value from stdin
    THolder<TFreeStringArg> ValueArg;

    virtual void BuildCommand(IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item("do").Scalar("write")
            .Item("path").Scalar(PathArg->getValue())
            .Item("value").OnNode(DeserializeFromYson(ValueArg->getValue()));

        TTransactedArgs::BuildCommand(consumer);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TUploadArgs
    : public TTransactedArgs
{
public:
    TUploadArgs()
    {
        PathArg.Reset(new TFreeStringArg("path", "path in Cypress", true, "", "path"));
        Cmd->add(~PathArg);
    }

private:
    THolder<TFreeStringArg> PathArg;

    virtual void BuildCommand(IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item("do").Scalar("upload")
            .Item("path").Scalar(PathArg->getValue());

        TTransactedArgs::BuildCommand(consumer);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TDownloadArgs
    : public TTransactedArgs
{
public:
    TDownloadArgs()
    {
        PathArg.Reset(new TFreeStringArg("path", "path in Cypress", true, "", "path"));
        Cmd->add(~PathArg);
    }

private:
    THolder<TFreeStringArg> PathArg;

    virtual void BuildCommand(IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item("do").Scalar("download")
            .Item("path").Scalar(PathArg->getValue());

        TTransactedArgs::BuildCommand(consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
