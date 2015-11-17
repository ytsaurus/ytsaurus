#pragma once

#include "command.h"

#include <ytlib/ypath/rich.h>

#include <ytlib/formats/format.h>

#include <ytlib/table_client/unversioned_row.h>

#include <ytlib/table_client/config.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TReadTableCommand
    : public TTypedCommand<NApi::TTableReaderOptions>
{
private:
    NYPath::TRichYPath Path;
    NYTree::INodePtr TableReader;
    NFormats::TControlAttributesConfigPtr ControlAttributes;
    bool Unordered;

    virtual void OnLoaded() override
    {
        TCommandBase::OnLoaded();

        Path = Path.Normalize();
    }

public:
    TReadTableCommand()
    {
        RegisterParameter("path", Path);
        RegisterParameter("table_reader", TableReader)
            .Default(nullptr);
        RegisterParameter("control_attributes", ControlAttributes)
            .DefaultNew();
        RegisterParameter("unordered", Unordered)
            .Default(false);
    }

    void Execute(ICommandContextPtr context);

};

class TWriteTableCommand
    : public TTypedCommand<NApi::TTransactionalOptions>
{
private:
    NYPath::TRichYPath Path;
    NYTree::INodePtr TableWriter;

    virtual void OnLoaded() override
    {
        TCommandBase::OnLoaded();

        Path = Path.Normalize();
    }

public:
    TWriteTableCommand()
    {
        RegisterParameter("path", Path);
        RegisterParameter("table_writer", TableWriter)
            .Default(nullptr);
    }

    void Execute(ICommandContextPtr context);

};

template <class TOptions>
class TTabletCommandBase
    : public TTypedCommand<TOptions>
{
protected:
    NYPath::TRichYPath Path;

    TTabletCommandBase()
    {
        this->RegisterParameter("path", Path);
        this->RegisterParameter("first_tablet_index", this->Options.FirstTabletIndex)
            .Default();
        this->RegisterParameter("last_tablet_index", this->Options.LastTabletIndex)
            .Default();
    }
};

class TMountTableCommand
    : public TTabletCommandBase<NApi::TMountTableOptions>
{
public:
    TMountTableCommand()
    {
        RegisterParameter("cell_id", Options.CellId)
            .Optional();
    }

    void Execute(ICommandContextPtr context);

};

class TUnmountTableCommand
    : public TTabletCommandBase<NApi::TUnmountTableOptions>
{
public:
    TUnmountTableCommand()
    {
        RegisterParameter("force", Options.Force)
            .Optional();
    }

    void Execute(ICommandContextPtr context);

};

class TRemountTableCommand
    : public TTabletCommandBase<NApi::TRemountTableOptions>
{
public:
    void Execute(ICommandContextPtr context);

};

class TReshardTableCommand
    : public TTabletCommandBase<NApi::TReshardTableOptions>
{
    std::vector<NTableClient::TOwningKey> PivotKeys;

public:
    TReshardTableCommand()
    {
        RegisterParameter("pivot_keys", PivotKeys);
    }

    void Execute(ICommandContextPtr context);

};

class TSelectRowsCommand
    : public TTypedCommandBase<NApi::TSelectRowsOptions>
{
private:
    Stroka Query;

public:
    TSelectRowsCommand()
    {
        RegisterParameter("query", Query);
        RegisterParameter("timestamp", Options.Timestamp)
            .Optional();
        RegisterParameter("input_row_limit", Options.InputRowLimit)
            .Optional();
        RegisterParameter("output_row_limit", Options.OutputRowLimit)
            .Optional();
        RegisterParameter("range_expansion_limit", Options.RangeExpansionLimit)
            .Optional();
        RegisterParameter("fail_on_incomplete_result", Options.FailOnIncompleteResult)
            .Optional();
        RegisterParameter("verbose_logging", Options.VerboseLogging)
            .Optional();
        RegisterParameter("enable_code_cache", Options.EnableCodeCache)
            .Optional();
        RegisterParameter("max_subqueries", Options.MaxSubqueries)
            .Optional();
    }

    void Execute(ICommandContextPtr context);

};

class TInsertRowsCommand
    : public TTypedCommandBase<NApi::TTransactionStartOptions>
{
private:
    NYTree::INodePtr TableWriter;
    NYPath::TRichYPath Path;
    bool Update;
    bool Aggregate;

public:
    TInsertRowsCommand()
    {
        RegisterParameter("table_writer", TableWriter)
            .Default();
        RegisterParameter("path", Path);
        RegisterParameter("update", Update)
            .Default(false);
        RegisterParameter("aggregate", Aggregate)
            .Default(false);
        RegisterParameter("atomicity", Options.Atomicity)
            .Optional();
        RegisterParameter("durability", Options.Durability)
            .Optional();
    }

    void Execute(ICommandContextPtr context);

};

class TLookupRowsCommand
    : public TTypedCommandBase<NApi::TLookupRowsOptions>
{
private:
    NYTree::INodePtr TableWriter;
    NYPath::TRichYPath Path;
    TNullable<std::vector<Stroka>> ColumnNames;

public:
    TLookupRowsCommand()
    {
        RegisterParameter("table_writer", TableWriter)
            .Default();
        RegisterParameter("path", Path);
        RegisterParameter("timestamp", Options.Timestamp)
            .Optional();
        RegisterParameter("column_names", ColumnNames)
            .Default();
    }

    void Execute(ICommandContextPtr context);

};

class TDeleteRowsCommand
    : public TTypedCommandBase<NApi::TTransactionStartOptions>
{
private:
    NYTree::INodePtr TableWriter;
    NYPath::TRichYPath Path;

public:
    TDeleteRowsCommand()
    {
        RegisterParameter("table_writer", TableWriter)
            .Default();
        RegisterParameter("path", Path);
        RegisterParameter("atomicity", Options.Atomicity)
            .Optional();
        RegisterParameter("durability", Options.Durability)
            .Optional();
    }

    void Execute(ICommandContextPtr context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
