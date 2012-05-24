#pragma once

#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/ypath_service.h>
#include <ytlib/ytree/ypath_client.h>

#include <ytlib/scheduler/config.h>

#include <ytlib/driver/public.h>
#include <ytlib/driver/config.h>
#include <ytlib/driver/format.h>

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

    virtual TError Execute(const std::vector<std::string>& args);

protected:
    struct TFormatDefaultsConfig
        : public TConfigurable
    {
        NYTree::INodePtr Structured;
        NYTree::INodePtr Tabular;

        TFormatDefaultsConfig()
        {
            Register("structured", Structured)
                .Default();
            Register("tabular", Tabular)
                .Default();
        }
    };

    typedef TIntrusivePtr<TFormatDefaultsConfig> TDefaultFormatConfigPtr;

    struct TConfig
        : public NDriver::TDriverConfig
    {
        NYTree::INodePtr Logging;
        TDefaultFormatConfigPtr FormatDefaults;
        TDuration OperationWaitTimeout;

        TConfig()
        {
            Register("logging", Logging);
            Register("format_defaults", FormatDefaults)
                .DefaultNew();
            Register("operation_wait_timeout", OperationWaitTimeout)
                .Default(TDuration::Seconds(3));
        }
    };

    typedef TIntrusivePtr<TConfig> TConfigPtr;

    typedef TCLAP::UnlabeledValueArg<Stroka> TUnlabeledStringArg;

    TCLAP::CmdLine CmdLine;
    TCLAP::ValueArg<Stroka> ConfigArg;
    TCLAP::ValueArg<Stroka> FormatArg;
    TCLAP::ValueArg<Stroka> InputFormatArg;
    TCLAP::ValueArg<Stroka> OutputFormatArg;
    TCLAP::MultiArg<Stroka> ConfigSetArg;
    TCLAP::MultiArg<Stroka> OptsArg;

    NYTree::IMapNodePtr ParseArgs(const std::vector<std::string>& args);
    
    Stroka GetConfigFileName();
    TConfigPtr ParseConfig();
    void ApplyConfigUpdates(NYTree::IYPathServicePtr service);
    
    NDriver::TFormat GetFormat(TConfigPtr config, NDriver::EDataType dataType, const NYTree::TYson& custom);

    void BuildOptions(NYTree::IYsonConsumer* consumer);

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
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

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

class TGetArgsParser
    : public TTransactedArgsParser
{
public:
    TGetArgsParser();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
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

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
};

//////////////////////////////////////////////////////////////////////////////////

class TRemoveArgsParser
    : public TTransactedArgsParser
{
public:
    TRemoveArgsParser();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
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

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
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

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
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

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
};

//////////////////////////////////////////////////////////////////////////////////

//class TStartTxArgsParser
//    : public TTransactedArgsParser
//{
//private:
//    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
//    virtual Stroka GetDriverCommandName() const;
//};

//////////////////////////////////////////////////////////////////////////////////

//class TRenewTxArgsParser
//    : public TTransactedArgsParser
//{
//private:
//    virtual Stroka GetDriverCommandName() const;
//};

//////////////////////////////////////////////////////////////////////////////////

//class TCommitTxArgsParser
//    : public TTransactedArgsParser
//{
//private:
//    virtual Stroka GetDriverCommandName() const;
//};

//////////////////////////////////////////////////////////////////////////////////

//class TAbortTxArgsParser
//    : public TTransactedArgsParser
//{
//private:
//    virtual Stroka GetDriverCommandName() const;
//};

//////////////////////////////////////////////////////////////////////////////////

//class TReadArgsParser
//    : public TTransactedArgsParser
//{
//public:
//    TReadArgsParser();

//private:
//    TUnlabeledStringArg PathArg;

//    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
//    virtual Stroka GetDriverCommandName() const;
//};

//////////////////////////////////////////////////////////////////////////////////

//class TWriteArgsParser
//    : public TTransactedArgsParser
//{
//public:
//    TWriteArgsParser();

//private:
//    TUnlabeledStringArg PathArg;
//    TUnlabeledStringArg ValueArg;
//    TCLAP::ValueArg<NYTree::TYson> KeyColumnsArg;
    
//    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
//    virtual Stroka GetDriverCommandName() const;
//};

//////////////////////////////////////////////////////////////////////////////////

//class TUploadArgsParser
//    : public TTransactedArgsParser
//{
//public:
//    TUploadArgsParser();

//private:
//    TUnlabeledStringArg PathArg;

//    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
//    virtual Stroka GetDriverCommandName() const;
//};

//////////////////////////////////////////////////////////////////////////////////

//class TDownloadArgsParser
//    : public TTransactedArgsParser
//{
//public:
//    TDownloadArgsParser();

//private:
//    TUnlabeledStringArg PathArg;

//    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
//    virtual Stroka GetDriverCommandName() const;
//};

//////////////////////////////////////////////////////////////////////////////////

//class TStartOpArgsParser
//    : public TTransactedArgsParser
//{
//public:
//    TStartOpArgsParser();

//    virtual TError Execute(const std::vector<std::string>& args);

//private:
//    class TOperationTracker;

//    TCLAP::SwitchArg NoTrackArg;

//    virtual NScheduler::EOperationType GetOperationType() const = 0;
//};

//////////////////////////////////////////////////////////////////////////////////

//class TMapArgsParser
//    : public TStartOpArgsParser
//{
//public:
//    TMapArgsParser();

//private:
//    TCLAP::MultiArg<Stroka> InArg;
//    TCLAP::MultiArg<Stroka> OutArg;
//    TCLAP::MultiArg<Stroka> FilesArg;
//    TCLAP::ValueArg<Stroka> MapperArg;

//    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
//    virtual Stroka GetDriverCommandName() const;
//    virtual NScheduler::EOperationType GetOperationType() const;
//};

//////////////////////////////////////////////////////////////////////////////////

//class TMergeArgsParser
//    : public TStartOpArgsParser
//{
//public:
//    TMergeArgsParser();

//private:
//    TCLAP::MultiArg<Stroka> InArg;
//    TCLAP::ValueArg<Stroka> OutArg;

//    typedef TNullable<NScheduler::EMergeMode> TMode;
//    typedef TCLAP::ValueArg<TMode> TModeArg;
//    TModeArg ModeArg;

//    TCLAP::SwitchArg CombineArg;

//    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
//    virtual Stroka GetDriverCommandName() const;
//    virtual NScheduler::EOperationType GetOperationType() const;
//};

//////////////////////////////////////////////////////////////////////////////////

//class TSortArgsParser
//    : public TStartOpArgsParser
//{
//public:
//    TSortArgsParser();

//private:
//    TCLAP::MultiArg<Stroka> InArg;
//    TCLAP::ValueArg<Stroka> OutArg;
//    TCLAP::ValueArg<NYTree::TYson> KeyColumnsArg;

//    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
//    virtual Stroka GetDriverCommandName() const;
//    virtual NScheduler::EOperationType GetOperationType() const;
//};

//////////////////////////////////////////////////////////////////////////////////

//class TEraseArgsParser
//    : public TStartOpArgsParser
//{
//public:
//    TEraseArgsParser();

//private:
//    TCLAP::ValueArg<Stroka> InArg;
//    TCLAP::ValueArg<Stroka> OutArg;

//    TCLAP::SwitchArg CombineArg;

//    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
//    virtual Stroka GetDriverCommandName() const;
//    virtual NScheduler::EOperationType GetOperationType() const;
//};

//////////////////////////////////////////////////////////////////////////////////

//class TAbortOpArgsParser
//    : public TArgsParserBase
//{
//public:
//    TAbortOpArgsParser();

//private:
//    TCLAP::ValueArg<Stroka> OpArg;

//    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
//    virtual Stroka GetDriverCommandName() const;
//};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
