#pragma once

#include "command.h"

#include <yt/ytlib/formats/format.h>

#include <yt/ytlib/table_client/config.h>
#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/ytlib/ypath/rich.h>

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
private:
    TNullable<std::vector<NTableClient::TOwningKey>> PivotKeys;
    TNullable<int> TabletCount;

public:
    TReshardTableCommand()
    {
        RegisterParameter("pivot_keys", PivotKeys)
            .Default();
        RegisterParameter("tablet_count", TabletCount)
            .Default()
            .GreaterThan(0);

        RegisterValidator([&] () {
            if (PivotKeys && TabletCount) {
                THROW_ERROR_EXCEPTION("Cannot specify both \"pivot_keys\" and \"tablet_count\"");
            }
            if (!PivotKeys && !TabletCount) {
                THROW_ERROR_EXCEPTION("Must specify either \"pivot_keys\" or \"tablet_count\"");
            }
        });
    }

    void Execute(ICommandContextPtr context);

};

class TAlterTableCommand
    : public TTypedCommand<NApi::TAlterTableOptions>
{
public: 
    TAlterTableCommand()
    {
        RegisterParameter("path", Path);
        RegisterParameter("schema", Options.Schema)
            .Optional();
    }

    void Execute(ICommandContextPtr context);

private:
    NYPath::TRichYPath Path;
};

class TSelectRowsCommand
    : public TTypedCommand<NApi::TSelectRowsOptions>
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
        RegisterParameter("workload_descriptor", Options.WorkloadDescriptor)
            .Optional();
    }

    void Execute(ICommandContextPtr context);

};

class TInsertRowsCommand
    : public TTypedCommand<NApi::TTransactionStartOptions>
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
    : public TTypedCommand<NApi::TLookupRowsOptions>
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
        RegisterParameter("keep_missing_rows", Options.KeepMissingRows)
            .Optional();
    }

    void Execute(ICommandContextPtr context);

};

class TDeleteRowsCommand
    : public TTypedCommand<NApi::TTransactionStartOptions>
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
