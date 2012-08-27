#pragma once

#include "command.h"

#include <ytlib/ytree/public.h>
#include <ytlib/table_client/public.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TReadRequest
    : public TTransactedRequest
{
    NYTree::TYPath Path;

    TReadRequest()
    {
        Register("path", Path);
    }
};

typedef TIntrusivePtr<TReadRequest> TReadRequestPtr;

class TReadCommand
    : public TTransactedCommandBase<TReadRequest>
{
public:
    explicit TReadCommand(ICommandContext* host)
        : TTransactedCommandBase(host)
        , TUntypedCommandBase(host)
    { }

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteRequest
    : public TTransactedRequest
{
    NYTree::TYPath Path;
    TNullable<NTableClient::TKeyColumns> SortedBy;

    TWriteRequest()
    {
        Register("path", Path);
        Register("sorted_by", SortedBy)
            .Default();
    }
};

typedef TIntrusivePtr<TWriteRequest> TWriteRequestPtr;

class TWriteCommand
    : public TTransactedCommandBase<TWriteRequest>
{
public:
    explicit TWriteCommand(ICommandContext* host)
        : TTransactedCommandBase(host)
        , TUntypedCommandBase(host)
    { }

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
