#pragma once

#include "executor.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TUploadExecutor
    : public TTransactedExecutor
{
public:
    TUploadExecutor();

private:
    TCLAP::UnlabeledValueArg<NYTree::TRichYPath> PathArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

//////////////////////////////////////////////////////////////////////////////////

class TDownloadExecutor
    : public TTransactedExecutor
{
public:
    TDownloadExecutor();

private:
    TCLAP::UnlabeledValueArg<NYTree::TRichYPath> PathArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
