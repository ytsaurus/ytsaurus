#pragma once

#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/ypath_service.h>
#include <ytlib/ytree/ypath_client.h>

#include <ytlib/misc/tclap_helpers.h>
#include <tclap/CmdLine.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TArgsBase
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TArgsBase> TPtr;

    TArgsBase();

    void Parse(std::vector<std::string>& args);

    NYTree::INodePtr GetCommand();

    Stroka GetConfigName();
    NYTree::EYsonFormat GetOutputFormat();

    void ApplyConfigUpdates(NYTree::IYPathService* service);

protected:
    // useful  typedefs
    typedef TCLAP::UnlabeledValueArg<Stroka> TUnlabeledStringArg;

    THolder<TCLAP::CmdLine> CmdLine;

    // config related
    THolder< TCLAP::ValueArg<Stroka> > ConfigArg;
    THolder< TCLAP::ValueArg<NYTree::EYsonFormat> > OutputFormatArg;
    THolder< TCLAP::MultiArg<Stroka> > ConfigUpdatesArg;

    THolder<TCLAP::MultiArg<Stroka> > OptsArg;

    void BuildOptions(NYTree::IYsonConsumer* consumer, TCLAP::MultiArg<Stroka>* arg);

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

typedef TIntrusivePtr<TArgsBase> TArgsBasePtr;

////////////////////////////////////////////////////////////////////////////////

class TTransactedArgs
    : public TArgsBase
{
public:
    TTransactedArgs();

protected:
    typedef TCLAP::ValueArg<NObjectServer::TTransactionId> TTxArg;
    THolder<TTxArg> TxArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TGetArgs
    : public TTransactedArgs
{
public:
    TGetArgs();

private:
    THolder<TUnlabeledStringArg> PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TSetArgs
    : public TTransactedArgs
{
public:
    TSetArgs();

private:
    THolder<TUnlabeledStringArg> PathArg;
    THolder<TUnlabeledStringArg> ValueArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TRemoveArgs
    : public TTransactedArgs
{
public:
    TRemoveArgs();

private:
    THolder<TUnlabeledStringArg> PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TListArgs
    : public TTransactedArgs
{
public:
    TListArgs();

private:
    THolder<TUnlabeledStringArg> PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TCreateArgs
    : public TTransactedArgs
{
public:
    TCreateArgs();

private:
    THolder<TUnlabeledStringArg> PathArg;

    typedef TCLAP::UnlabeledValueArg<NObjectServer::EObjectType> TTypeArg;
    THolder<TTypeArg> TypeArg;

    typedef TCLAP::ValueArg<NYTree::TYson> TManifestArg;
    THolder<TManifestArg> ManifestArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

class TLockArgs
    : public TTransactedArgs
{
public:
    TLockArgs();

private:
    THolder<TUnlabeledStringArg> PathArg;

    typedef TCLAP::ValueArg<NCypress::ELockMode> TModeArg;
    THolder<TModeArg> ModeArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

class TStartTxArgs
    : public TArgsBase
{
public:
    TStartTxArgs();

private:
    typedef TCLAP::ValueArg<NYTree::TYson> TManifestArg;
    THolder<TManifestArg> ManifestArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TCommitTxArgs
    : public TTransactedArgs
{
private:
    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TAbortTxArgs
    : public TTransactedArgs
{
private:
    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TReadArgs
    : public TTransactedArgs
{
public:
    TReadArgs();

private:
    THolder<TUnlabeledStringArg> PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

class TWriteArgs
    : public TTransactedArgs
{
public:
    TWriteArgs();

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
    THolder<TUnlabeledStringArg> PathArg;

    // TODO(panin): support value from stdin
    THolder<TUnlabeledStringArg> ValueArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

class TUploadArgs
    : public TTransactedArgs
{
public:
    TUploadArgs();

private:
    THolder<TUnlabeledStringArg> PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

class TDownloadArgs
    : public TTransactedArgs
{
public:
    TDownloadArgs();

private:
    THolder<TUnlabeledStringArg> PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TMapArgs
    : public TTransactedArgs
{
public:
    TMapArgs();

private:
    THolder<TCLAP::MultiArg<Stroka> > InArg;
    THolder<TCLAP::MultiArg<Stroka> > OutArg;
    THolder<TCLAP::ValueArg<Stroka> > ShellCommandArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
