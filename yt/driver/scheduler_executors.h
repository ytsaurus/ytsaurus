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

    virtual EExitCode DoExecute(const NDriver::TDriverRequest& request) override;

    virtual NScheduler::EOperationType GetOperationType() const = 0;
};

//////////////////////////////////////////////////////////////////////////////////

class TMapExecutor
    : public TStartOpExecutor
{
public:
    TMapExecutor();

private:
    TCLAP::MultiArg<NYTree::TRichYPath> InArg;
    TCLAP::MultiArg<NYTree::TRichYPath> OutArg;
    TCLAP::ValueArg<Stroka> CommandArg;
    TCLAP::MultiArg<NYTree::TRichYPath> FileArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
    virtual NScheduler::EOperationType GetOperationType() const override;
};

//////////////////////////////////////////////////////////////////////////////////

class TMergeExecutor
    : public TStartOpExecutor
{
public:
    TMergeExecutor();

private:
    TCLAP::MultiArg<NYTree::TRichYPath> InArg;
    TCLAP::ValueArg<NYTree::TRichYPath> OutArg;

    typedef TNullable<NScheduler::EMergeMode> TMode;
    typedef TCLAP::ValueArg<TMode> TModeArg;
    TModeArg ModeArg;

    TCLAP::SwitchArg CombineArg;
    TCLAP::ValueArg<Stroka> MergeByArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
    virtual NScheduler::EOperationType GetOperationType() const override;
};

//////////////////////////////////////////////////////////////////////////////////

class TSortExecutor
    : public TStartOpExecutor
{
public:
    TSortExecutor();

private:
    TCLAP::MultiArg<NYTree::TRichYPath> InArg;
    TCLAP::ValueArg<NYTree::TRichYPath> OutArg;
    TCLAP::ValueArg<Stroka> SortByArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
    virtual NScheduler::EOperationType GetOperationType() const override;
};

//////////////////////////////////////////////////////////////////////////////////

class TEraseExecutor
    : public TStartOpExecutor
{
public:
    TEraseExecutor();

private:
    TCLAP::UnlabeledValueArg<NYTree::TRichYPath> PathArg;
    TCLAP::SwitchArg CombineArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
    virtual NScheduler::EOperationType GetOperationType() const override;
};

//////////////////////////////////////////////////////////////////////////////////

class TReduceExecutor
    : public TStartOpExecutor
{
public:
    TReduceExecutor();

private:
    TCLAP::MultiArg<NYTree::TRichYPath> InArg;
    TCLAP::MultiArg<NYTree::TRichYPath> OutArg;
    TCLAP::ValueArg<Stroka> CommandArg;
    TCLAP::MultiArg<NYTree::TRichYPath> FileArg;
    TCLAP::ValueArg<Stroka> ReduceByArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
    virtual NScheduler::EOperationType GetOperationType() const override;
};

//////////////////////////////////////////////////////////////////////////////////

class TMapReduceExecutor
    : public TStartOpExecutor
{
public:
    TMapReduceExecutor();

private:
    TCLAP::MultiArg<NYTree::TRichYPath> InArg;
    TCLAP::MultiArg<NYTree::TRichYPath> OutArg;
    TCLAP::ValueArg<Stroka> MapperCommandArg;
    TCLAP::MultiArg<NYTree::TRichYPath> MapperFileArg;
    TCLAP::ValueArg<Stroka> ReducerCommandArg;
    TCLAP::MultiArg<NYTree::TRichYPath> ReducerFileArg;
    TCLAP::ValueArg<Stroka> SortByArg;
    TCLAP::ValueArg<Stroka> ReduceByArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
    virtual NScheduler::EOperationType GetOperationType() const override;
};

//////////////////////////////////////////////////////////////////////////////////

class TAbortOpExecutor
    : public TRequestExecutor
{
public:
    TAbortOpExecutor();

private:
    TCLAP::UnlabeledValueArg<Stroka> OpArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) override override;
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TTrackOpExecutor
    : public TRequestExecutor
{
public:
    TTrackOpExecutor();


private:
    TCLAP::UnlabeledValueArg<Stroka> OpArg;

    virtual EExitCode DoExecute() override;
    virtual Stroka GetCommandName() const override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
