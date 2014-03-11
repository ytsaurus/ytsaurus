#include "stdafx.h"
#include "rowset.h"

#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/schemaful_writer.h>
#include <ytlib/new_table_client/row_buffer.h>

#include <ytlib/tablet_client/wire_protocol.h>

namespace NYT {
namespace NApi {

using namespace NTabletClient;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

class TRowsetBase
    : public IRowset
{
public:
    // IRowset implementation.
    virtual const TTableSchema& GetSchema() const override
    {
        return Schema_;
    }

    virtual const TNameTablePtr& GetNameTable() const override
    {
        if (!NameTable_) {
            NameTable_ = TNameTable::FromSchema(Schema_);
        }
        return NameTable_;
    }

    virtual const std::vector<TUnversionedRow>& GetRows() const override
    {
        return Rows_;
    }

protected:
    TTableSchema Schema_;
    mutable TNameTablePtr NameTable_; // create on demand
    std::vector<TUnversionedRow> Rows_;


};

////////////////////////////////////////////////////////////////////////////////

class TRowset
    : public TRowsetBase
{
public:
    TRowset(
        std::vector<std::unique_ptr<TWireProtocolReader>> readers,
        const TTableSchema& schema,
        std::vector<TUnversionedRow> rows)
        : Readers_(std::move(readers))
    {
        Schema_ = schema;
        Rows_ = std::move(rows);
    }

private:
    std::vector<std::unique_ptr<TWireProtocolReader>> Readers_;

};

IRowsetPtr CreateRowset(
    std::vector<std::unique_ptr<NTabletClient::TWireProtocolReader>> readers,
    const TTableSchema& schema,
    std::vector<TUnversionedRow> rows)
{   
    return New<TRowset>(
        std::move(readers),
        schema,
        std::move(rows));
}

////////////////////////////////////////////////////////////////////////////////

static const auto PresetResult = MakeFuture(TError());

class TSchemafulRowsetWriter
    : public TRowsetBase
    , public ISchemafulWriter
{
public:
    TSchemafulRowsetWriter()
        : Result_(NewPromise<TErrorOr<IRowsetPtr>>())
    { }

    TPromise<TErrorOr<IRowsetPtr>> GetResult() const
    {
        return Result_;
    }

    // ISchemafulWriter implementation.
    virtual TAsyncError Open(
        const TTableSchema& schema,
        const TNullable<TKeyColumns>& /*keyColumns*/) override
    {
        Schema_ = schema;
        return PresetResult;
    }

    virtual TAsyncError Close() override
    {
        Result_.Set(IRowsetPtr(this));
        Result_.Reset();
        return PresetResult;
    }

    virtual bool Write(const std::vector<TUnversionedRow>& rows) override
    {
        for (auto row : rows) {
            Rows_.push_back(RowBuffer_.Capture(row));
        }
        return true;
    }

    virtual TAsyncError GetReadyEvent() override
    {
        return PresetResult;
    }

private:
    TPromise<TErrorOr<IRowsetPtr>> Result_;

    TRowBuffer RowBuffer_;

};

std::tuple<ISchemafulWriterPtr, TPromise<TErrorOr<IRowsetPtr>>> CreateSchemafulRowsetWriter()
{
    auto writer = New<TSchemafulRowsetWriter>();
    return std::make_tuple(writer, writer->GetResult());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

