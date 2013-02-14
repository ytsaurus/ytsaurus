#pragma once

#include "command.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TParseYPathRequest
    : public TRequestBase
{
    Stroka Path;

    TParseYPathRequest()
    {
        Register("path", Path);
    }
};

class TParseYPathCommand
    : public TTypedCommandBase<TParseYPathRequest>
{
public:
    explicit TParseYPathCommand(ICommandContext* context)
        : TTypedCommandBase(context)
    { }

private:
    virtual void DoExecute() override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
