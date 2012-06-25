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
    TUnlabeledStringArg PathArg;

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
    TUnlabeledStringArg PathArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
