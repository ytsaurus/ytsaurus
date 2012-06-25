#pragma once

#include "executor.h"

namespace NYT {
namespace NDriver {

//////////////////////////////////////////////////////////////////////////////////

class TStartOpExecutor
    : public TTransactedExecutor
{
public:
    TStartOpExecutor();

private:
    TCLAP::SwitchArg DontTrackArg;

    virtual EExitCode DoExecute(const NDriver::TDriverRequest& request);

    virtual NScheduler::EOperationType GetOperationType() const = 0;
};

//////////////////////////////////////////////////////////////////////////////////

class TMapExecutor
    : public TStartOpExecutor
{
public:
    TMapExecutor();

private:
    TCLAP::MultiArg<Stroka> InArg;
    TCLAP::MultiArg<Stroka> OutArg;
    TCLAP::MultiArg<Stroka> FilesArg;
    TCLAP::ValueArg<Stroka> MapperArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetCommandName() const;
    virtual NScheduler::EOperationType GetOperationType() const;
};

//////////////////////////////////////////////////////////////////////////////////

class TMergeExecutor
    : public TStartOpExecutor
{
public:
    TMergeExecutor();

private:
    TCLAP::MultiArg<Stroka> InArg;
    TCLAP::ValueArg<Stroka> OutArg;

    typedef TNullable<NScheduler::EMergeMode> TMode;
    typedef TCLAP::ValueArg<TMode> TModeArg;
    TModeArg ModeArg;

    TCLAP::SwitchArg CombineArg;
    TCLAP::ValueArg<NYTree::TYson> KeyColumnsArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetCommandName() const;
    virtual NScheduler::EOperationType GetOperationType() const;
};

//////////////////////////////////////////////////////////////////////////////////

class TSortExecutor
    : public TStartOpExecutor
{
public:
    TSortExecutor();

private:
    TCLAP::MultiArg<Stroka> InArg;
    TCLAP::ValueArg<Stroka> OutArg;
    TCLAP::ValueArg<NYTree::TYson> KeyColumnsArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetCommandName() const;
    virtual NScheduler::EOperationType GetOperationType() const;
};

//////////////////////////////////////////////////////////////////////////////////

class TEraseExecutor
    : public TStartOpExecutor
{
public:
    TEraseExecutor();

private:
    TUnlabeledStringArg PathArg;
    TCLAP::SwitchArg CombineArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetCommandName() const;
    virtual NScheduler::EOperationType GetOperationType() const;
};

//////////////////////////////////////////////////////////////////////////////////

class TReduceExecutor
    : public TStartOpExecutor
{
public:
    TReduceExecutor();

private:
    TCLAP::MultiArg<Stroka> InArg;
    TCLAP::MultiArg<Stroka> OutArg;
    TCLAP::MultiArg<Stroka> FilesArg;
    TCLAP::ValueArg<Stroka> ReducerArg;
    TCLAP::ValueArg<NYTree::TYson> KeyColumnsArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetCommandName() const;
    virtual NScheduler::EOperationType GetOperationType() const;
};

//////////////////////////////////////////////////////////////////////////////////

class TAbortOpExecutor
    : public TExecutor
{
public:
    TAbortOpExecutor();

private:
    TCLAP::ValueArg<Stroka> OpArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

class TTrackOpExecutor
    : public TExecutor
{
public:
    TTrackOpExecutor();

    virtual EExitCode Execute(const std::vector<std::string>& args);

private:
    TCLAP::ValueArg<Stroka> OpArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetCommandName() const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
