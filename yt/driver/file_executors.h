#pragma once

#include "executor.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TUploadExecutor
    : public TTransactedExecutor
{
public:
    TUploadExecutor();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
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
    virtual Stroka GetDriverCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
