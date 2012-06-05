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
    virtual Stroka GetDriverCommandName() const;
};

//////////////////////////////////////////////////////////////////////////////////

class TWriteExecutor
    : public TTransactedExecutor
{
public:
    TWriteExecutor();

private:
    TUnlabeledStringArg PathArg;
    TUnlabeledStringArg ValueArg;
    TCLAP::ValueArg<NYTree::TYson> SortedBy;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
