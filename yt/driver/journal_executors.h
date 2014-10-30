#pragma once

#include "executor.h"

#include <ytlib/ypath/rich.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TReadJournalExecutor
    : public TTransactedExecutor
{
public:
    TReadJournalExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

//////////////////////////////////////////////////////////////////////////////////

class TWriteJournalExecutor
    : public TTransactedExecutor
{
public:
    TWriteJournalExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;
    TUnlabeledStringArg ValueArg;

    bool UseStdIn;
    TStringStream Stream;

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
    virtual TInputStream* GetInputStream() override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
