#pragma once

#include "command.h"

#include <ytlib/ypath/rich.h>

#include <ytlib/table_client/public.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TReadRequest
    : public TTransactionalRequest
{
    NYPath::TRichYPath Path;
    NYTree::INodePtr TableReaderConfig;

    TReadRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("table_reader", TableReaderConfig)
            .Default(nullptr);
    }
};

typedef TIntrusivePtr<TReadRequest> TReadRequestPtr;

class TReadCommand
    : public TTypedCommandBase<TReadRequest>
    , public TTransactionalCommandMixin
{
public:
    explicit TReadCommand(ICommandContext* context)
        : TTypedCommandBase(context)
        , TTransactionalCommandMixin(context, Request)
    { }

private:
    virtual void DoExecute();

};

////////////////////////////////////////////////////////////////////////////////

struct TWriteRequest
    : public TTransactionalRequest
{
    NYPath::TRichYPath Path;
    TNullable<NTableClient::TKeyColumns> SortedBy;
    NYTree::INodePtr TableWriterConfig;

    TWriteRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("table_writer", TableWriterConfig)
            .Default(nullptr);
    }
};

typedef TIntrusivePtr<TWriteRequest> TWriteRequestPtr;

class TWriteCommand
    : public TTypedCommandBase<TWriteRequest>
    , public TTransactionalCommandMixin
{
public:
    explicit TWriteCommand(ICommandContext* context)
        : TTypedCommandBase(context)
        , TTransactionalCommandMixin(context, Request)
    { }

private:
    virtual void DoExecute();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
