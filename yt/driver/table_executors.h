#pragma once

#include "executor.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TReadExecutor
    : public TTransactedExecutor
{
public:
    TReadExecutor();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetCommandName() const;
};

//////////////////////////////////////////////////////////////////////////////////

class TWriteExecutor
    : public TTransactedExecutor
{
public:
    TWriteExecutor();

private:
    TUnlabeledStringArg PathArg;
    // TODO(panin) : think of extracting common part of this and TSetExecutor
    TUnlabeledStringArg ValueArg;

    TCLAP::ValueArg<NYTree::TYson> SortedBy;

    bool UseStdIn;
    TStringStream Stream;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetCommandName() const;
    virtual TInputStream* GetInputStream();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
