#pragma once

#include <ytlib/ytree/tree_builder.h>

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
        consumer->OnMapItem("transaction_id");
        consumer->OnStringScalar(TxArg->getValue().ToString());
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
        PathArg.Reset(new TFreeStringArg("path", "path in cypress", true, "", "string"));
        Cmd->add(~PathArg);
    }

private:
    THolder<TFreeStringArg> PathArg;

    virtual void BuildCommand(IYsonConsumer* consumer)
    {
        consumer->OnMapItem("do");
        consumer->OnStringScalar("get");

        consumer->OnMapItem("path");
        consumer->OnStringScalar(PathArg->getValue());

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
        PathArg.Reset(new TFreeStringArg("path", "path in cypress", true, "", "string"));
        ValueArg.Reset(new TFreeStringArg("value", "value to set", true, "", "yson"));

        Cmd->add(~PathArg);
        Cmd->add(~ValueArg);
    }

    virtual void BuildCommand(IYsonConsumer* consumer)
    {
        consumer->OnMapItem("do");
        consumer->OnStringScalar("set");

        consumer->OnMapItem("path");
        consumer->OnStringScalar(PathArg->getValue());

        consumer->OnMapItem("value");
        TStringInput input(ValueArg->getValue());
        ParseYson(&input, consumer);

        TTransactedArgs::BuildCommand(consumer);
    }

private:
    THolder<TFreeStringArg> PathArg;
    THolder<TFreeStringArg> ValueArg;
};

////////////////////////////////////////////////////////////////////////////////

class TRemoveArgs
    : public TTransactedArgs
{
public:
    TRemoveArgs()
    {
        PathArg.Reset(new TFreeStringArg("path", "path in cypress", true, "", "string"));
        Cmd->add(~PathArg);
    }

    virtual void BuildCommand(IYsonConsumer* consumer)
    {
        consumer->OnMapItem("do");
        consumer->OnStringScalar("remove");

        consumer->OnMapItem("path");
        consumer->OnStringScalar(PathArg->getValue());

        TTransactedArgs::BuildCommand(consumer);
    }


private:
    THolder<TFreeStringArg> PathArg;
};

////////////////////////////////////////////////////////////////////////////////

class TListArgs
    : public TTransactedArgs
{
public:
    TListArgs()
    {
        PathArg.Reset(new TFreeStringArg("path", "path in cypress", true, "", "string"));
        Cmd->add(~PathArg);
    }

private:
    THolder<TFreeStringArg> PathArg;
};

////////////////////////////////////////////////////////////////////////////////

class TCreateArgs
    : public TTransactedArgs
{
public:
    TCreateArgs()
    {
        PathArg.Reset(new TFreeStringArg("path", "path in cypress", true, "", "string"));
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
};

////////////////////////////////////////////////////////////////////////////////

class TLockArgs
    : public TTransactedArgs
{
public:
    TLockArgs()
    {
        PathArg.Reset(new TFreeStringArg("path", "path in cypress", true, "", "string"));
        ModeArg.Reset(new TModeArg(
            "", "mode", "lock mode", false, NCypress::ELockMode::Exclusive, "snapshot, shared, exclusive"));
        Cmd->add(~PathArg);
        Cmd->add(~ModeArg);
    }

private:
    THolder<TFreeStringArg> PathArg;

    typedef TCLAP::ValueArg<NCypress::ELockMode> TModeArg;
    THolder<TModeArg> ModeArg;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
