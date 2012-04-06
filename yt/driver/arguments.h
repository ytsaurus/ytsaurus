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

class TArgsParserBase
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TArgsParserBase> TPtr;

    TArgsParserBase();

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

    void BuildOptions(NYTree::IYsonConsumer* consumer);
    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

typedef TIntrusivePtr<TArgsParserBase> TArgsBasePtr;

////////////////////////////////////////////////////////////////////////////////

class TTransactedArgsParser
    : public TArgsParserBase
{
public:
    TTransactedArgsParser();

protected:
    typedef TCLAP::ValueArg<NObjectServer::TTransactionId> TTxArg;
    TTxArg TxArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TGetArgsParser
    : public TTransactedArgsParser
{
public:
    TGetArgsParser();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TSetArgsParser
    : public TTransactedArgsParser
{
public:
    TSetArgsParser();

private:
    TUnlabeledStringArg PathArg;
    TUnlabeledStringArg ValueArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TRemoveArgsParser
    : public TTransactedArgsParser
{
public:
    TRemoveArgsParser();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TListArgsParser
    : public TTransactedArgsParser
{
public:
    TListArgsParser();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TCreateArgsParser
    : public TTransactedArgsParser
{
public:
    TCreateArgsParser();

private:
    typedef TCLAP::UnlabeledValueArg<NObjectServer::EObjectType> TTypeArg;
    TTypeArg TypeArg;

    TUnlabeledStringArg PathArg;

    typedef TCLAP::ValueArg<NYTree::TYson> TManifestArg;
    TManifestArg ManifestArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

class TLockArgsParser
    : public TTransactedArgsParser
{
public:
    TLockArgsParser();

private:
    TUnlabeledStringArg PathArg;

    typedef TCLAP::ValueArg<NCypress::ELockMode> TModeArg;
    TModeArg ModeArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

class TStartTxArgsParser
    : public TTransactedArgsParser
{
public:
    TStartTxArgsParser();

private:
    typedef TCLAP::ValueArg<NYTree::TYson> TManifestArg;
    TManifestArg ManifestArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TCommitTxArgsParser
    : public TTransactedArgsParser
{
public:
    TCommitTxArgsParser()
    { }

private:
    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TAbortTxArgsParser
    : public TTransactedArgsParser
{
private:
    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TReadArgsParser
    : public TTransactedArgsParser
{
public:
    TReadArgsParser();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

class TWriteArgsParser
    : public TTransactedArgsParser
{
public:
    TWriteArgsParser();

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

class TUploadArgsParser
    : public TTransactedArgsParser
{
public:
    TUploadArgsParser();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

class TDownloadArgsParser
    : public TTransactedArgsParser
{
public:
    TDownloadArgsParser();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

class TMapArgsParser
    : public TTransactedArgsParser
{
public:
    TMapArgsParser();

private:
    TCLAP::MultiArg<Stroka> InArg;
    TCLAP::MultiArg<Stroka> OutArg;
    TCLAP::MultiArg<Stroka> FilesArg;
    TCLAP::ValueArg<Stroka> MapperArg;

    virtual void BuildCommand(NYTree::IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

class TMergeArgsParser
    : public TTransactedArgsParser
{
public:
    TMergeArgsParser();

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
