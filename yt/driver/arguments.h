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

    typedef TNullable<NYTree::EYsonFormat> TFormat;
    TFormat GetOutputFormat();

    void ApplyConfigUpdates(NYTree::IYPathService* service);

protected:
    //useful typedefs
    typedef TCLAP::UnlabeledValueArg<Stroka> TFreeStringArg;

    TCLAP::CmdLine Cmd;

    // config related
    TCLAP::ValueArg<std::string> ConfigArg;

    typedef TCLAP::ValueArg< TFormat > TOutputFormatArg;
    TOutputFormatArg OutputFormatArg;

    TCLAP::MultiArg<Stroka> ConfigUpdatesArg;

    TCLAP::MultiArg<std::string> OptsArg;

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
    TTxArg TxArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TGetArgs
    : public TTransactedArgs
{
public:
    TGetArgs();

private:
    TFreeStringArg PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TSetArgs
    : public TTransactedArgs
{
public:
    TSetArgs();

private:
    TFreeStringArg PathArg;
    TFreeStringArg ValueArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TRemoveArgs
    : public TTransactedArgs
{
public:
    TRemoveArgs();

private:
    TFreeStringArg PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TListArgs
    : public TTransactedArgs
{
public:
    TListArgs();

private:
    TFreeStringArg PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TCreateArgs
    : public TTransactedArgs
{
public:
    TCreateArgs();

private:
    TFreeStringArg PathArg;

    typedef TCLAP::UnlabeledValueArg<NObjectServer::EObjectType> TTypeArg;
    TTypeArg TypeArg;

    typedef TCLAP::ValueArg<NYTree::TYson> TManifestArg;
    TManifestArg ManifestArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

class TLockArgs
    : public TTransactedArgs
{
public:
    TLockArgs();

private:
    TFreeStringArg PathArg;

    typedef TCLAP::ValueArg<NCypress::ELockMode> TModeArg;
    TModeArg ModeArg;

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
    TManifestArg ManifestArg;

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
    TFreeStringArg PathArg;

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
    TFreeStringArg PathArg;

    //TODO(panin):support value from stdin
    TFreeStringArg ValueArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

class TUploadArgs
    : public TTransactedArgs
{
public:
    TUploadArgs();

private:
    TFreeStringArg PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

class TDownloadArgs
    : public TTransactedArgs
{
public:
    TDownloadArgs();

private:
    TFreeStringArg PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
