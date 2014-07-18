#pragma once

#include "command.h"

#include <ytlib/table_client/public.h>

#include <ytlib/ypath/rich.h>

#include <ytlib/formats/format.h>

#include <ytlib/new_table_client/unversioned_row.h>

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

struct TTabletRequest
    : public TRequest
{
    NYPath::TRichYPath Path;
    TNullable<int> FirstTabletIndex;
    TNullable<int> LastTabletIndex;

    TTabletRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("first_tablet_index", FirstTabletIndex)
            .Default(Null);
        RegisterParameter("last_tablet_index", LastTabletIndex)
            .Default(Null);
    }
};

struct TMountTableRequest
    : public TTabletRequest
{ };

class TMountTableCommand
    : public TTypedCommand<TMountTableRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TUnmountTableRequest
    : public TTabletRequest
{
    bool Force;

    TUnmountTableRequest()
    {
        RegisterParameter("force", Force)
            .Default(false);
    }
};

class TUnmountTableCommand
    : public TTypedCommand<TUnmountTableRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TRemountTableRequest
    : public TTabletRequest
{ };

class TRemountTableCommand
    : public TTypedCommand<TRemountTableRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TReshardTableRequest
    : public TRequest
{
    NYPath::TRichYPath Path;
    TNullable<int> FirstTabletIndex;
    TNullable<int> LastTabletIndex;
    std::vector<NVersionedTableClient::TOwningKey> PivotKeys;

    TReshardTableRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("first_tablet_index", FirstTabletIndex)
            .Default(Null);
        RegisterParameter("last_tablet_index", LastTabletIndex)
            .Default(Null);
        RegisterParameter("pivot_keys", PivotKeys);
    }
};

class TReshardTableCommand
    : public TTypedCommand<TReshardTableRequest>
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
    NVersionedTableClient::TTimestamp Timestamp;
    TNullable<i64> InputRowLimit;
    TNullable<i64> OutputRowLimit;

    TSelectRequest()
    {
        RegisterParameter("query", Query);
        RegisterParameter("timestamp", Timestamp)
            .Default(NVersionedTableClient::LastCommittedTimestamp);
        RegisterParameter("input_row_limit", InputRowLimit)
            .Default();
        RegisterParameter("output_row_limit", OutputRowLimit)
            .Default();
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
    NVersionedTableClient::TOwningKey Key;
    NTransactionClient::TTimestamp Timestamp;
    TNullable<std::vector<Stroka>> ColumnNames;

    TLookupRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("key", Key);
        RegisterParameter("timestamp", Timestamp)
            .Default(NTransactionClient::LastCommittedTimestamp);
        RegisterParameter("column_names", ColumnNames)
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
    NVersionedTableClient::TOwningKey Key;

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
