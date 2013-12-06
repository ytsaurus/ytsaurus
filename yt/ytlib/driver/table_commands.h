#pragma once

#include "command.h"

#include <ytlib/table_client/public.h>

#include <ytlib/ypath/rich.h>

#include <ytlib/formats/format.h>

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

class TReadCommand
    : public TTypedCommand<TReadRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TWriteRequest
    : public TTransactionalRequest
{
    NYPath::TRichYPath Path;
    NYTree::INodePtr TableWriter;

    TWriteRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("table_writer", TableWriter)
            .Default(nullptr);
    }
};

class TWriteCommand
    : public TTypedCommand<TWriteRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TMountRequest
    : public TRequest
{
    NYPath::TRichYPath Path;

    TMountRequest()
    {
        RegisterParameter("path", Path);
    }
};

class TMountCommand
    : public TTypedCommand<TMountRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TUnmountRequest
    : public TRequest
{
    NYPath::TRichYPath Path;

    TUnmountRequest()
    {
        RegisterParameter("path", Path);
    }
};

class TUnmountCommand
    : public TTypedCommand<TUnmountRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TInsertRequest
    : public TRequest
{
    NYPath::TRichYPath Path;
    bool Update;
    NYTree::INodePtr TableWriter;

    TInsertRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("update", Update)
            .Default(false);
        RegisterParameter("table_writer", TableWriter)
            .Default(nullptr);
    }
};

class TInsertCommand
    : public TTypedCommand<TInsertRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TSelectRequest
    : public TRequest
{
    Stroka Query;

    TSelectRequest()
    {
        RegisterParameter("query", Query);
    }
};

class TSelectCommand
    : public TTypedCommand<TSelectRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TLookupRequest
    : public TRequest
{
    NYPath::TRichYPath Path;
    std::vector<NYTree::INodePtr> Key;
    NTransactionClient::TTimestamp Timestamp;
    TNullable<std::vector<Stroka>> Columns;

    TLookupRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("key", Key);
        RegisterParameter("timestamp", Timestamp)
            .Default(NTransactionClient::LastCommittedTimestamp);
        RegisterParameter("columns", Columns)
            .Default(Null);
    }
};

class TLookupCommand
    : public TTypedCommand<TLookupRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TDeleteRequest
    : public TRequest
{
    NYPath::TRichYPath Path;
    std::vector<NYTree::INodePtr> Key;

    TDeleteRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("key", Key);
    }
};

class TDeleteCommand
    : public TTypedCommand<TDeleteRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
