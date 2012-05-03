#pragma once

#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/ypath_service.h>
#include <ytlib/ytree/ypath_client.h>

#include <ytlib/scheduler/config.h>

#include <ytlib/driver/public.h>
#include <ytlib/driver/config.h>

#include <ytlib/misc/tclap_helpers.h>
#include <tclap/CmdLine.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TArgsParserBase
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TArgsParserBase> TPtr;

    struct TConfig
        : public NDriver::TDriverConfig
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        NYTree::INodePtr Logging;
        TDuration OperationWaitTimeout;

        TConfig()
        {
            Register("logging", Logging);
            Register("operation_wait_timeout", OperationWaitTimeout)
                .Default(TDuration::Seconds(3));
        }
    };

    TArgsParserBase();

    virtual TError Execute(
        const std::vector<std::string>& args,
        NYTree::INodePtr configNode);

    Stroka GetConfigFileName();

    typedef TNullable<NYTree::EYsonFormat> TFormat;
    TFormat GetOutputFormat();

    void ApplyConfigUpdates(NYTree::IYPathServicePtr service);

protected:
    class TPassthroughDriverHost;
    class TInterceptingDriverHost;

    // useful typedefs
    typedef TCLAP::UnlabeledValueArg<Stroka> TUnlabeledStringArg;

    TCLAP::CmdLine CmdLine;

    // config related
    TCLAP::ValueArg<Stroka> ConfigArg;

    typedef TCLAP::ValueArg<TFormat> TOutputFormatArg;
    TOutputFormatArg OutputFormatArg;

    TCLAP::MultiArg<Stroka> ConfigSetArg;

    TCLAP::MultiArg<Stroka> OptsArg;

    NYTree::INodePtr ParseArgs(const std::vector<std::string>& args);
    TConfig::TPtr ParseConfig(NYTree::INodePtr configNode);

    void BuildOptions(NYTree::IYsonConsumer* consumer);

    virtual void BuildRequest(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const = 0;
};

typedef TIntrusivePtr<TArgsParserBase> TArgsBasePtr;

////////////////////////////////////////////////////////////////////////////////

class TTransactedArgsParser
    : public TArgsParserBase
{
public:
    TTransactedArgsParser();

protected:
    TCLAP::ValueArg<Stroka> TxArg;

    virtual void BuildRequest(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TGetArgsParser
    : public TTransactedArgsParser
{
public:
    TGetArgsParser();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildRequest(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
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

    virtual void BuildRequest(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

class TRemoveArgsParser
    : public TTransactedArgsParser
{
public:
    TRemoveArgsParser();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildRequest(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

class TListArgsParser
    : public TTransactedArgsParser
{
public:
    TListArgsParser();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildRequest(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
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

    virtual void BuildRequest(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
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

    virtual void BuildRequest(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

class TStartTxArgsParser
    : public TTransactedArgsParser
{
private:
    virtual void BuildRequest(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

class TCommitTxArgsParser
    : public TTransactedArgsParser
{
private:
    virtual Stroka GetDriverCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

class TAbortTxArgsParser
    : public TTransactedArgsParser
{
private:
    virtual Stroka GetDriverCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

class TReadArgsParser
    : public TTransactedArgsParser
{
public:
    TReadArgsParser();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildRequest(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

class TWriteArgsParser
    : public TTransactedArgsParser
{
public:
    TWriteArgsParser();

private:
    TUnlabeledStringArg PathArg;
    TUnlabeledStringArg ValueArg;
    TCLAP::ValueArg<NYTree::TYson> KeyColumnsArg;
    
    virtual void BuildRequest(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

class TUploadArgsParser
    : public TTransactedArgsParser
{
public:
    TUploadArgsParser();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildRequest(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

class TDownloadArgsParser
    : public TTransactedArgsParser
{
public:
    TDownloadArgsParser();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildRequest(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

class TStartOpArgsParser
    : public TTransactedArgsParser
{
public:
    TStartOpArgsParser();

    virtual TError Execute(
        const std::vector<std::string>& args,
        NYTree::INodePtr configNode);

private:
    class TOperationTracker;

    TCLAP::SwitchArg NoTrackArg;

};

////////////////////////////////////////////////////////////////////////////////

class TMapArgsParser
    : public TStartOpArgsParser
{
public:
    TMapArgsParser();

private:
    TCLAP::MultiArg<Stroka> InArg;
    TCLAP::MultiArg<Stroka> OutArg;
    TCLAP::MultiArg<Stroka> FilesArg;
    TCLAP::ValueArg<Stroka> MapperArg;

    virtual void BuildRequest(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

class TMergeArgsParser
    : public TStartOpArgsParser
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

    virtual void BuildRequest(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

class TSortArgsParser
    : public TStartOpArgsParser
{
public:
    TSortArgsParser();

private:
    TCLAP::MultiArg<Stroka> InArg;
    TCLAP::ValueArg<Stroka> OutArg;
    TCLAP::ValueArg<NYTree::TYson> KeyColumnsArg;

    virtual void BuildRequest(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

class TEraseArgsParser
    : public TStartOpArgsParser
{
public:
    TEraseArgsParser();

private:
    TCLAP::ValueArg<Stroka> InArg;
    TCLAP::ValueArg<Stroka> OutArg;

    TCLAP::SwitchArg CombineArg;

    virtual void BuildRequest(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

class TAbortOpArgsParser
    : public TArgsParserBase
{
public:
    TAbortOpArgsParser();

private:
    TCLAP::ValueArg<Stroka> OpArg;

    virtual void BuildRequest(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
