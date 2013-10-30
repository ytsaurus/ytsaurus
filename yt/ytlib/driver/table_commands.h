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
    TNullable<NTableClient::TKeyColumns> SortedBy;
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

    void DoExecuteMounted(TTableMountInfoPtr mountInfo);
    void DoExecuteNotMounted();

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

} // namespace NDriver
} // namespace NYT
