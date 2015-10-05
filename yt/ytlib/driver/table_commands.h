#pragma once

#include "command.h"

#include <ytlib/ypath/rich.h>

#include <ytlib/formats/format.h>

#include <ytlib/table_client/unversioned_row.h>

#include <ytlib/table_client/config.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TReadTableRequest
    : public TTransactionalRequest
{
    NYPath::TRichYPath Path;
    NYTree::INodePtr TableReader;
    NTableClient::TControlAttributesConfigPtr ControlAttributes;
    bool Unordered;

    TReadTableRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("table_reader", TableReader)
            .Default(nullptr);
        RegisterParameter("control_attributes", ControlAttributes)
            .DefaultNew();
        RegisterParameter("unordered", Unordered)
            .Default(false);
    }

    virtual void OnLoaded() override
    {
        TTransactionalRequest::OnLoaded();
        
        Path = Path.Normalize();
    }
};

class TReadTableCommand
    : public TTypedCommand<TReadTableRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TWriteTableRequest
    : public TTransactionalRequest
{
    NYPath::TRichYPath Path;
    NYTree::INodePtr TableWriter;

    TWriteTableRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("table_writer", TableWriter)
            .Default(nullptr);
    }

    virtual void OnLoaded() override
    {
        TTransactionalRequest::OnLoaded();
        
        Path = Path.Normalize();
    }
};

class TWriteTableCommand
    : public TTypedCommand<TWriteTableRequest>
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
            .Default();
        RegisterParameter("last_tablet_index", LastTabletIndex)
            .Default();
    }
};

struct TMountTableRequest
    : public TTabletRequest
{
    NTabletClient::TTabletCellId CellId;

    TMountTableRequest()
    {
        RegisterParameter("cell_id", CellId)
            .Default(NTabletClient::NullTabletCellId);
    }
};

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
    : public TTabletRequest
{
    std::vector<NTableClient::TOwningKey> PivotKeys;

    TReshardTableRequest()
    {
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

struct TSelectRowsRequest
    : public TRequest
{
    Stroka Query;
    NTableClient::TTimestamp Timestamp;
    TNullable<i64> InputRowLimit;
    TNullable<i64> OutputRowLimit;
    ui64 RangeExpansionLimit;
    bool FailOnIncompleteResult;
    bool VerboseLogging;
    bool EnableCodeCache;
    int MaxSubqueries;

    TSelectRowsRequest()
    {
        RegisterParameter("query", Query);
        RegisterParameter("timestamp", Timestamp)
            .Default(NTableClient::SyncLastCommittedTimestamp);
        RegisterParameter("input_row_limit", InputRowLimit)
            .Default();
        RegisterParameter("output_row_limit", OutputRowLimit)
            .Default();
        RegisterParameter("range_expansion_limit", RangeExpansionLimit)
            .Default(1000);
        RegisterParameter("fail_on_incomplete_result", FailOnIncompleteResult)
            .Default(true);
        RegisterParameter("verbose_logging", VerboseLogging)
            .Default(false);
        RegisterParameter("enable_code_cache", EnableCodeCache)
            .Default(true);
        RegisterParameter("max_subqueries", MaxSubqueries)
            .Default(std::numeric_limits<int>::max());
    }
};

class TSelectRowsCommand
    : public TTypedCommand<TSelectRowsRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TInsertRowsRequest
    : public TRequest
{
    NYTree::INodePtr TableWriter;
    NYPath::TRichYPath Path;
    bool Update;
    NTransactionClient::EAtomicity Atomicity;
    NTransactionClient::EDurability Durability;

    TInsertRowsRequest()
    {
        RegisterParameter("table_writer", TableWriter)
            .Default();
        RegisterParameter("path", Path);
        RegisterParameter("update", Update)
            .Default(false);
        RegisterParameter("atomicity", Atomicity)
            .Default(NTransactionClient::EAtomicity::Full);
        RegisterParameter("durability", Durability)
            .Default(NTransactionClient::EDurability::Sync);
    }
};

class TInsertRowsCommand
    : public TTypedCommand<TInsertRowsRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TLookupRowsRequest
    : public TRequest
{
    NYTree::INodePtr TableWriter;
    NYPath::TRichYPath Path;
    NTransactionClient::TTimestamp Timestamp;
    TNullable<std::vector<Stroka>> ColumnNames;

    TLookupRowsRequest()
    {
        RegisterParameter("table_writer", TableWriter)
            .Default();
        RegisterParameter("path", Path);
        RegisterParameter("timestamp", Timestamp)
            .Default(NTransactionClient::SyncLastCommittedTimestamp);
        RegisterParameter("column_names", ColumnNames)
            .Default();
    }
};

class TLookupRowsCommand
    : public TTypedCommand<TLookupRowsRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TDeleteRowsRequest
    : public TRequest
{
    NYTree::INodePtr TableWriter;
    NYPath::TRichYPath Path;

    TDeleteRowsRequest()
    {
        RegisterParameter("table_writer", TableWriter)
            .Default();
        RegisterParameter("path", Path);
    }
};

class TDeleteRowsCommand
    : public TTypedCommand<TDeleteRowsRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
