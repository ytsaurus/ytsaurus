#pragma once

#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/serialize.h>

#include <tclap/CmdLine.h>
#include <ytlib/misc/tclap_helpers.h>

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

protected:
    //useful typedefs
    typedef TCLAP::UnlabeledValueArg<Stroka> TFreeStringArg;

    THolder<TCLAP::CmdLine> Cmd;

    THolder<TCLAP::ValueArg<std::string> > ConfigArg;
    THolder<TCLAP::ValueArg<NYTree::EYsonFormat> > FormatArg;

    THolder<TCLAP::MultiArg<std::string> > OptsArg;

    void BuildOpts(NYTree::IYsonConsumer* consumer);
    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

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
    THolder<TFreeStringArg> PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TSetArgs
    : public TTransactedArgs
{
public:
    TSetArgs();

private:
    THolder<TFreeStringArg> PathArg;
    THolder<TFreeStringArg> ValueArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TRemoveArgs
    : public TTransactedArgs
{
public:
    TRemoveArgs();

private:
    THolder<TFreeStringArg> PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TListArgs
    : public TTransactedArgs
{
public:
    TListArgs();

private:
    THolder<TFreeStringArg> PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TCreateArgs
    : public TTransactedArgs
{
public:
    TCreateArgs();

private:
    THolder<TFreeStringArg> PathArg;

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
    THolder<TFreeStringArg> PathArg;

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
public:
    TCommitTxArgs()
    { }

private:

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TAbortTxArgs
    : public TTransactedArgs
{
public:
    TAbortTxArgs()
    { }

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
    THolder<TFreeStringArg> PathArg;

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
    THolder<TFreeStringArg> PathArg;

    //TODO(panin):support value from stdin
    THolder<TFreeStringArg> ValueArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

class TUploadArgs
    : public TTransactedArgs
{
public:
    TUploadArgs();

private:
    THolder<TFreeStringArg> PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

class TDownloadArgs
    : public TTransactedArgs
{
public:
    TDownloadArgs();

private:
    THolder<TFreeStringArg> PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
