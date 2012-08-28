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

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetCommandName() const;
};

//////////////////////////////////////////////////////////////////////////////////

class TDownloadExecutor
    : public TTransactedExecutor
{
public:
    TDownloadExecutor();

private:
    TCLAP::UnlabeledValueArg<NYTree::TRichYPath> PathArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
