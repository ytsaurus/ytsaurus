#pragma once

#include "command.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TStartRequest
    : public TRequestBase
{
    NYTree::INodePtr Manifest;

    TStartRequest()
    {
        Register("manifest", Manifest)
            .Default();
    }
};

class TStartCommand
    : public TCommandBase<TStartRequest>
{
public:
    TStartCommand(ICommandHost* commandHost)
        : TCommandBase(commandHost)
    { }

private:
    virtual void DoExecute(TStartRequest* request);
};

////////////////////////////////////////////////////////////////////////////////

struct TCommitRequest
    : public TTransactedRequest
{ };

class TCommitCommand
    : public TCommandBase<TCommitRequest>
{
public:
    TCommitCommand(ICommandHost* commandHost)
        : TCommandBase(commandHost)
    { }

private:
    virtual void DoExecute(TCommitRequest* request);
};

////////////////////////////////////////////////////////////////////////////////

struct TAbortRequest
    : public TTransactedRequest
{ };

class TAbortCommand
    : public TCommandBase<TAbortRequest>
{
public:
    TAbortCommand(ICommandHost* commandHost)
        : TCommandBase(commandHost)
    { }

private:
    virtual void DoExecute(TAbortRequest* request);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

