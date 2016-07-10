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
        RegisterParameter("freeze", Options.Freeze)
            .Default(false);
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

class TFreezeTableCommand
    : public TTabletCommandBase<NApi::TFreezeTableOptions>
{
public:
    void Execute(ICommandContextPtr context);

};

class TUnfreezeTableCommand
    : public TTabletCommandBase<NApi::TUnfreezeTableOptions>
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
        RegisterParameter("dynamic", Options.Dynamic)
            .Optional();
    }

    void Execute(ICommandContextPtr context);

private:
    NYPath::TRichYPath Path;
};

struct TSelectRowsOptions
    : public NApi::TSelectRowsOptions
    , public TTabletReadOptions
{ };

class TSelectRowsCommand
    : public TTypedCommand<TSelectRowsOptions>
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
    : public TTypedCommand<TTabletWriteOptions>
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
    }

    void Execute(ICommandContextPtr context);

};

struct TLookupRowsOptions
    : public NApi::TLookupRowsOptions
    , public TTabletReadOptions
{ };

class TLookupRowsCommand
    : public TTypedCommand<TLookupRowsOptions>
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
        RegisterParameter("column_names", ColumnNames)
            .Default();
        RegisterParameter("timestamp", Options.Timestamp)
            .Optional();
        RegisterParameter("keep_missing_rows", Options.KeepMissingRows)
            .Optional();
    }

    void Execute(ICommandContextPtr context);

};

class TDeleteRowsCommand
    : public TTypedCommand<TTabletWriteOptions>
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
    }

    void Execute(ICommandContextPtr context);

};

class TTrimRowsCommand
    : public TTypedCommand<NApi::TTrimTableOptions>
{
private:
    NYPath::TRichYPath Path;
    int TabletIndex;
    i64 TrimmedRowCount;

public:
    TTrimRowsCommand()
    {
        RegisterParameter("path", Path);
        RegisterParameter("tablet_index", TabletIndex);
        RegisterParameter("trimmed_row_count", TrimmedRowCount);
    }

    void Execute(ICommandContextPtr context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
