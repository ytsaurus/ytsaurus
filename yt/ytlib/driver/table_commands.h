#pragma once

#include "command.h"

#include <ytlib/table_client/public.h>
#include <ytlib/ypath/rich.h>
#include <ytlib/misc/intrusive_ptr.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TReadRequest
    : public TTransactionalRequest
{
    NYPath::TRichYPath Path;
    NYTree::INodePtr TableReader;

    TReadRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("table_reader", TableReader)
            .Default(nullptr);
    }
};

typedef TIntrusivePtr<TReadRequest> TReadRequestPtr;


class TReadSession;

class TReadCommand
    : public TTypedCommand<TReadRequest>
{
public:
    TReadCommand();

    ~TReadCommand();

private:
    virtual void DoExecute();

    TIntrusivePtr<TReadSession> Session_;
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteRequest
    : public TTransactionalRequest
{
    NYPath::TRichYPath Path;
    TNullable<NTableClient::TKeyColumns> SortedBy;
    NYTree::INodePtr TableWriter;

    TWriteRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("table_writer", TableWriter)
            .Default(nullptr);
    }
};

typedef TIntrusivePtr<TWriteRequest> TWriteRequestPtr;

class TWriteSession;

class TWriteCommand
    : public TTypedCommand<TWriteRequest>
{
public:
    TWriteCommand();

    ~TWriteCommand();

private:
    virtual void DoExecute();

    TIntrusivePtr<TWriteSession> Session_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
