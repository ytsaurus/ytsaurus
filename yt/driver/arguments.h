#pragma once

#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/ypath_service.h>
#include <ytlib/ytree/ypath_client.h>

#include <ytlib/scheduler/config.h>

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

    void ApplyConfigUpdates(NYTree::IYPathServicePtr service);

protected:
    // useful typedefs
    typedef TCLAP::UnlabeledValueArg<Stroka> TUnlabeledStringArg;

    TCLAP::CmdLine CmdLine;

    // config related
    TCLAP::ValueArg<Stroka> ConfigArg;

    typedef TCLAP::ValueArg<TFormat> TOutputFormatArg;
    TOutputFormatArg OutputFormatArg;

    TCLAP::MultiArg<Stroka> ConfigUpdatesArg;

    TCLAP::MultiArg<Stroka> OptsArg;

    void BuildOptions(NYTree::IYsonConsumer* consumer, TCLAP::MultiArg<Stroka>& arg);
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
    TUnlabeledStringArg PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TSetArgs
    : public TTransactedArgs
{
public:
    TSetArgs();

private:
    TUnlabeledStringArg PathArg;
    TUnlabeledStringArg ValueArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TRemoveArgs
    : public TTransactedArgs
{
public:
    TRemoveArgs();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TListArgs
    : public TTransactedArgs
{
public:
    TListArgs();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TCreateArgs
    : public TTransactedArgs
{
public:
    TCreateArgs();

private:
    typedef TCLAP::UnlabeledValueArg<NObjectServer::EObjectType> TTypeArg;
    TTypeArg TypeArg;

    TUnlabeledStringArg PathArg;

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
    TUnlabeledStringArg PathArg;

    typedef TCLAP::ValueArg<NCypress::ELockMode> TModeArg;
    TModeArg ModeArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

class TStartTxArgs
    : public TTransactedArgs
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
    TUnlabeledStringArg PathArg;

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
    TUnlabeledStringArg PathArg;
    TUnlabeledStringArg ValueArg;
    TCLAP::SwitchArg SortedArg;
    
    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

class TUploadArgs
    : public TTransactedArgs
{
public:
    TUploadArgs();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

class TDownloadArgs
    : public TTransactedArgs
{
public:
    TDownloadArgs();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

class TMapArgs
    : public TTransactedArgs
{
public:
    TMapArgs();

private:
    TCLAP::MultiArg<Stroka> InArg;
    TCLAP::MultiArg<Stroka> OutArg;
    TCLAP::MultiArg<Stroka> FilesArg;
    TCLAP::ValueArg<Stroka> MapperArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

class TMergeArgs
    : public TTransactedArgs
{
public:
    TMergeArgs();

private:
    TCLAP::MultiArg<Stroka> InArg;
    TCLAP::ValueArg<Stroka> OutArg;

    typedef TNullable<NScheduler::EMergeMode> TMode;
    typedef TCLAP::ValueArg<TMode> TModeArg;
    TModeArg ModeArg;

    TCLAP::SwitchArg CombineArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
