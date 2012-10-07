#pragma once

#include "executor.h"

#include <ytlib/ypath/rich.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TReadExecutor
    : public TTransactedExecutor
{
public:
    TReadExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

//////////////////////////////////////////////////////////////////////////////////

class TWriteExecutor
    : public TTransactedExecutor
{
public:
    TWriteExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;
    TUnlabeledStringArg ValueArg;
    TCLAP::ValueArg<Stroka> SortedBy;

    bool UseStdIn;
    TStringStream Stream;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
    virtual TInputStream* GetInputStream() override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
