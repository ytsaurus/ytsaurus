#pragma once

#include "executor.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TWriteFileExecutor
    : public TTransactedExecutor
{
public:
    TWriteFileExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

//////////////////////////////////////////////////////////////////////////////////

class TReadFileExecutor
    : public TTransactedExecutor
{
public:
    TReadFileExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

//////////////////////////////////////////////////////F//////////////////////////

} // namespace NDriver
} // namespace NYT
