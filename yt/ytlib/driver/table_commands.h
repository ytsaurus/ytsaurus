#pragma once

#include "command.h"

#include <ytlib/ypath/rich.h>

#include <ytlib/table_client/public.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TReadRequest
    : public TTransactedRequest
{
    NYPath::TRichYPath Path;
    NYTree::INodePtr TableReaderConfig;

    TReadRequest()
    {
        Register("path", Path);
        Register("table_reader", TableReaderConfig)
            .Default(NULL);
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
    NYPath::TRichYPath Path;
    TNullable<NTableClient::TKeyColumns> SortedBy;
    NYTree::INodePtr TableWriterConfig;

    TWriteRequest()
    {
        Register("path", Path);
        Register("sorted_by", SortedBy)
            .Default();
        Register("table_writer", TableWriterConfig)
            .Default(NULL);
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
